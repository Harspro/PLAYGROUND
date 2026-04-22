import json
import util.constants as consts
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import bigquery
from tsys_processing.tsys_file_loader_base import TsysFileLoader
from util.bq_utils import (
    create_external_table,
    apply_column_transformation,
    apply_timestamp_transformation,
    apply_execution_id_transformation,
    apply_schema_sync_transformation,
    get_execution_id
)


class TsysADMFileLoader(TsysFileLoader):
    def __init__(self, config_filename: str, config_dir: str = None):
        super().__init__(config_filename, config_dir)

    def apply_transformations(self, bigquery_client, transformation_config: dict, execution_id: str):
        print(transformation_config)
        transformed_data = create_external_table(bigquery_client, transformation_config.get(consts.EXTERNAL_TABLE_ID),
                                                 transformation_config.get(consts.DATA_FILE_LOCATION))

        transformed_data = apply_execution_id_transformation(bigquery_client, transformed_data, execution_id)

        add_columns = transformation_config.get(consts.ADD_COLUMNS)
        drop_columns = transformation_config.get(consts.DROP_COLUMNS)

        if add_columns or drop_columns:
            column_transform_view = apply_column_transformation(bigquery_client, transformed_data, add_columns,
                                                                drop_columns)
            transformed_data = column_transform_view

        transformed_data = apply_timestamp_transformation(bigquery_client, transformed_data)

        target_table_id = transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)
        transformed_data = apply_schema_sync_transformation(bigquery_client, transformed_data, target_table_id)

        return transformed_data

    def build_execution_id_registering_job(self, file_name: str, dag_id: str, env: str, **context):
        bigquery_client = bigquery.Client()
        exec_id = get_execution_id(bigquery_client, file_name, dag_id, env)
        context['ti'].xcom_push(key=consts.EXECUTION_ID, value=f'{exec_id}')

    def build_execution_id_registering_task(self, dag_id: str):
        return PythonOperator(
            task_id=consts.REGISTER_EXECUTION_ID,
            python_callable=self.build_execution_id_registering_job,
            op_kwargs={'file_name': "{{ dag_run.conf['name']}}",
                       'dag_id': dag_id,
                       'env': self.deploy_env}
        )

    def build_preprocessing_task_group(self, dag_id: str, cluster_name: str, dag_config: dict):
        with TaskGroup(group_id='preprocessing') as preprocessing_group:
            filtering_task = self.build_filtering_task(cluster_name, dag_config)
            exec_id_registering_task = self.build_execution_id_registering_task(dag_id)

            filtering_task >> exec_id_registering_task

        return preprocessing_group

    def build_load_result_clean_up_job(self, bigquery_config: dict, file_name: str, execution_id: str):
        load_result_table_id = self.get_load_result_table_id(bigquery_config)
        bigquery_client = bigquery.Client()
        deletion = f"""
            DELETE FROM `{load_result_table_id}`
            WHERE TABLE_NM='LOAD_RESULT'
            AND FILENAME='{file_name}'
            AND EXECUTION_ID<={execution_id}
        """

        bigquery_client.query(deletion).result()

    def build_postprocessing_task_group(self, dag_id: str, dag_config: dict):
        bigquery_config = dag_config.get(consts.BIGQUERY)
        with TaskGroup(group_id='postprocessing') as postprocessing_group:
            load_result_clean_up_task = PythonOperator(
                task_id=consts.CLEAN_UP_PLACEHOLDERS,
                python_callable=self.build_load_result_clean_up_job,
                op_kwargs={'bigquery_config': bigquery_config,
                           'file_name': "{{ dag_run.conf['name']}}",
                           'execution_id': f"{{{{ ti.xcom_pull(task_ids='preprocessing.{consts.REGISTER_EXECUTION_ID}', key='{consts.EXECUTION_ID}') }}}}"}
            )

            control_record_saving_task = PythonOperator(
                task_id='save_job_to_control_table',
                trigger_rule='none_failed',
                python_callable=self.build_control_record_saving_job,
                op_kwargs={'dag_id': dag_id,
                           'file_name': "{{ dag_run.conf['name']}}",
                           'output_dir': self.get_output_dir_path(dag_config),
                           'tables_info': self.extract_tables_info(dag_config)}
            )

            load_result_clean_up_task >> control_record_saving_task

        return postprocessing_group

    def validate_row_count(self, bigquery_client, bq_ext_table_id: str, segment_name: str,
                           record_count_column: str, context, tsys_count: str):
        if self.is_trailer(segment_name):
            print("########## inside if condition for trailer #######")
            query = f"SELECT * FROM {bq_ext_table_id}"
            results = bigquery_client.query(query).result().to_dataframe().to_json()
            context['ti'].xcom_push(key=f"{consts.RECORD_COUNT}", value=f'{results}')
        elif tsys_count > 0:
            print("########## inside else condition for other than trailer #######")
            query = f"SELECT COUNT(1) AS {record_count_column} FROM {bq_ext_table_id}"
            results = bigquery_client.query(query).result().to_dataframe()
            num = results[record_count_column].values[0]
            if int(tsys_count) != int(num):
                raise AirflowException(f"tsys count {tsys_count} does not match bigquery count {num}")

    def get_load_result_table_id(self, bigquery_config: dict):
        bq_project_name = bigquery_config.get(consts.PROJECT_ID) or self.gcp_config[consts.LANDING_ZONE_PROJECT_ID]
        bq_dataset_name = bigquery_config.get(consts.DATASET_ID)

        return f"{bq_project_name}.{bq_dataset_name}.LOAD_RESULT"

    def build_segment_load_result_job(self, bigquery_config: dict, bq_ext_table_id: str, segment_name: str,
                                      file_name, execution_id, **context):
        bigquery_client = bigquery.Client()
        table_config = bigquery_config.get(consts.TABLES).get(segment_name)
        record_count_column = table_config.get(consts.RECORD_COUNT_COLUMN)
        tsys_count = self.get_tsys_count(context, segment_name, record_count_column)
        self.validate_row_count(bigquery_client, bq_ext_table_id, segment_name, record_count_column, context,
                                tsys_count)
        load_result_table_id = self.get_load_result_table_id(bigquery_config)
        table_config = bigquery_config.get(consts.TABLES).get(segment_name)
        table_name = table_config.get(consts.TABLE_NAME)

        query = f"SELECT {consts.FILE_CREATE_DT} FROM {bq_ext_table_id} LIMIT 1"
        results = bigquery_client.query(query).result().to_dataframe()
        file_create_dt = results[consts.FILE_CREATE_DT].values[0]

        if '/' in file_name:
            file_name = file_name.rsplit('/', 1)[1]

        insertion = f"""
            INSERT INTO `{load_result_table_id}` (TABLE_NM, LOAD_ERRORS, REC_LOAD_DT_TM_STAMP, FILE_CREATE_DT, EXECUTION_ID, FILENAME, REC_CNT)
            VALUES ('{table_name}', 0, CURRENT_DATETIME('America/Toronto'), '{file_create_dt}', {execution_id}, '{file_name}', {tsys_count})
        """

        bigquery_client.query(insertion).result()

    def get_tsys_count(self, context, segment_name: str, record_count_column):
        tsys_count = 1
        if not self.is_trailer(segment_name):
            tsys_count_str = context['ti'].xcom_pull(task_ids=f"{consts.TRAILER_SEGMENT_NAME}.populate_load_result",
                                                     key=f"{consts.RECORD_COUNT}")
            tsys_count_json = json.loads(tsys_count_str)
            tsys_count = tsys_count_json.get(record_count_column).get('0')
        return tsys_count

    def build_segment_load_result_task(self, bigquery_config: dict, bq_ext_table_id: str, segment_name: str):
        return PythonOperator(
            task_id="populate_load_result",
            python_callable=self.build_segment_load_result_job,
            op_kwargs={'bigquery_config': bigquery_config,
                       'bq_ext_table_id': bq_ext_table_id,
                       'segment_name': segment_name,
                       'file_name': "{{ dag_run.conf['name']}}",
                       'execution_id': f"{{{{ ti.xcom_pull(task_ids='preprocessing.{consts.REGISTER_EXECUTION_ID}', key='{consts.EXECUTION_ID}') }}}}"}
        )

    def check_record_count(self, bigquery_config: dict, segment_name: str, **context):
        table_config = bigquery_config.get(consts.TABLES).get(segment_name)
        record_count_column = table_config.get(consts.RECORD_COUNT_COLUMN)
        if not self.is_trailer(segment_name):
            tsys_count_str = context['ti'].xcom_pull(task_ids=f"{consts.TRAILER_SEGMENT_NAME}.populate_load_result",
                                                     key=f"{consts.RECORD_COUNT}")
            tsys_count_json = json.loads(tsys_count_str)
            tsys_count = tsys_count_json.get(record_count_column).get('0')
            if tsys_count > 0:
                return f'{segment_name}.parse_file'
            else:
                return f'{segment_name}.populate_load_result'
        else:
            return f'{segment_name}.parse_file'

    def build_record_count_check(self, bigquery_config: dict, segment_name: str):
        return BranchPythonOperator(
            task_id="record_count_check",
            python_callable=self.check_record_count,
            op_kwargs={
                'bigquery_config': bigquery_config,
                'segment_name': segment_name
            })

    def build_segment_task_group(self, cluster_name: str, dag_config: dict, segment_name: str):
        spark_config = dag_config.get(consts.SPARK)
        parsing_job_config = spark_config.get(consts.PARSING_JOB_ARGS).copy()
        parsing_job_config['pcb.tsys.processor.datafile.path'] = self.get_input_file_path(dag_config)
        segment_configs = spark_config.get(consts.SEGMENT_ARGS)
        segment_config = segment_configs.get(segment_name)

        bigquery_config = dag_config.get(consts.BIGQUERY)

        parsing_job_config['pcb.tsys.processor.datafile.path'] = self.get_input_file_path(dag_config)
        output_dir = self.get_output_dir_path(dag_config)
        parsing_job_config['pcb.tsys.processor.output.path'] = f"{output_dir}/{segment_name}"

        with TaskGroup(group_id=segment_name) as segment_task_group:
            parsing_task = self.build_segment_parsing_task(cluster_name, spark_config, parsing_job_config, segment_config)
            transformation_config = self.build_transformation_config(bigquery_config, output_dir, segment_name)

            loading_task = self.build_segment_loading_task(bigquery_config, transformation_config)
            load_result_populating_task = self.build_segment_load_result_task(bigquery_config,
                                                                              transformation_config.get(
                                                                                  consts.EXTERNAL_TABLE_ID),
                                                                              segment_name)
            check_record_count_task = self.build_record_count_check(bigquery_config, segment_name)
            check_record_count_task >> parsing_task >> loading_task >> load_result_populating_task
            check_record_count_task >> load_result_populating_task

        return segment_task_group

    def build_segment_loading_job(self, bigquery_config: dict,
                                  transformation_config: dict, execution_id: str):
        bigquery_client = bigquery.Client()
        transformed_view = self.apply_transformations(bigquery_client, transformation_config, execution_id)

        data_load_type = bigquery_config.get(consts.DATA_LOAD_TYPE)

        if data_load_type == consts.APPEND_ONLY:
            loading_sql = f"""
                CREATE TABLE IF NOT EXISTS
                    `{transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)}`
                AS
                    SELECT {transformed_view.get(consts.COLUMNS)}
                    FROM {transformed_view.get(consts.ID)}
                    LIMIT 0;

                INSERT INTO `{transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)}`
                SELECT {transformed_view.get(consts.COLUMNS)}
                FROM {transformed_view.get(consts.ID)};
            """
        else:
            loading_sql = f"""
                CREATE TABLE
                    `{transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)}`
                {transformation_config.get(consts.DESTINATION_TABLE).get(consts.PARTITION)}
                {transformation_config.get(consts.DESTINATION_TABLE).get(consts.CLUSTERING)}
                AS
                    SELECT {transformed_view.get(consts.COLUMNS)}
                    FROM {transformed_view.get(consts.ID)};
            """

        print(loading_sql)
        query_job = bigquery_client.query(loading_sql)
        query_job.result()

    def build_segment_loading_task(self, bigquery_config: dict, transformation_config: dict):
        return PythonOperator(
            task_id="load_into_bq",
            python_callable=self.build_segment_loading_job,
            op_kwargs={'file_name': "{{ dag_run.conf['name']}}",
                       'bigquery_config': bigquery_config,
                       'transformation_config': transformation_config,
                       'execution_id': f"{{{{ ti.xcom_pull(task_ids='preprocessing.{consts.REGISTER_EXECUTION_ID}', key='{consts.EXECUTION_ID}') }}}}"}
        )
