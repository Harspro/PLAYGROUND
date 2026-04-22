import json
from datetime import timedelta, datetime
import logging

import util.constants as consts
from airflow.exceptions import AirflowException
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import bigquery
from tsys_processing.tsys_file_loader_base import TSYS_FILE_LOADER
from tsys_processing.tsys_file_loader_base import TsysFileLoader
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from util.bq_utils import (
    create_external_table,
    apply_column_transformation,
    apply_timestamp_transformation,
    apply_schema_sync_transformation,
    table_exists)
from util.deploy_utils import (
    read_pause_unpause_setting,
    pause_unpause_dag,
    read_feature_flag
)
from util.miscutils import (
    save_job_to_control_table,
    get_cluster_name_for_dag,
    get_cluster_config_by_job_size)
from dag_factory.terminus_dag_factory import add_tags


class TsysAccScoresFullFileLoader(TsysFileLoader):
    def __init__(self, config_filename: str, config_dir: str = None):
        super().__init__(config_filename, config_dir)
        self.ephemeral_cluster_name = "acc-scores-mdpc-ephemeral"

    def apply_transformations(self, bigquery_client, transformation_config: dict):
        print(transformation_config)
        transformed_data = create_external_table(bigquery_client, transformation_config.get(consts.EXTERNAL_TABLE_ID),
                                                 transformation_config.get(consts.DATA_FILE_LOCATION))
        add_columns = transformation_config.get(consts.ADD_COLUMNS)
        drop_columns = transformation_config.get(consts.DROP_COLUMNS)

        if add_columns or drop_columns:
            column_transform_view = apply_column_transformation(bigquery_client, transformed_data, add_columns,
                                                                drop_columns)
            transformed_data = column_transform_view
        target_table_id = transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)
        if table_exists(bigquery_client, target_table_id):
            transformed_data = apply_schema_sync_transformation(bigquery_client, transformed_data, target_table_id)

        return transformed_data

    def build_preprocessing_task_group(self, dag_id: str, cluster_name: str, dag_config: dict):
        return self.build_filtering_task(cluster_name, dag_config)

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

    def get_tsys_count(self, context, segment_name: str, record_count_column):
        tsys_count = 1
        if not self.is_trailer(segment_name):
            tsys_count_str = context['ti'].xcom_pull(task_ids=f"{consts.TRAILER_SEGMENT_NAME}.rec_count_validate",
                                                     key=f"{consts.RECORD_COUNT}")
            tsys_count_json = json.loads(tsys_count_str)
            tsys_count = tsys_count_json.get(record_count_column).get('0')
        return tsys_count

    def validate_rec_count_job(self, bigquery_config: dict, bq_ext_table_id: str, segment_name: str, **context):
        bigquery_client = bigquery.Client()
        table_config = bigquery_config.get(consts.TABLES).get(segment_name)
        record_count_column = table_config.get(consts.RECORD_COUNT_COLUMN)
        if record_count_column or self.is_trailer(segment_name):
            tsys_count = self.get_tsys_count(context, segment_name, record_count_column)
            self.validate_row_count(bigquery_client, bq_ext_table_id, segment_name, record_count_column, context,
                                    tsys_count)

    def validate_rec_count_task(self, bigquery_config: dict, bq_ext_table_id: str, segment_name: str):
        return PythonOperator(
            task_id="rec_count_validate",
            python_callable=self.validate_rec_count_job,
            op_kwargs={
                'bigquery_config': bigquery_config,
                'bq_ext_table_id': bq_ext_table_id,
                'segment_name': segment_name}
        )

    def build_segment_loading_job(self, bigquery_config: dict, transformation_config: dict, segment_name: str):
        bigquery_client = bigquery.Client()
        transformed_view = self.apply_transformations(bigquery_client, transformation_config)

        data_load_type = bigquery_config.get(consts.DATA_LOAD_TYPE)
        if self.is_trailer(segment_name) and table_exists(bigquery_client, transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)):
            self.duplicate_trailer_check(bigquery_config, transformation_config, transformed_view, segment_name)

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
        print(loading_sql)
        query_job = bigquery_client.query(loading_sql)
        query_job.result()

    def duplicate_trailer_check(self, bigquery_config: dict, transformation_config: dict, transformed_view: dict,
                                segment_name: str):
        bigquery_client = bigquery.Client()
        table_config = bigquery_config.get(consts.TABLES).get(segment_name)
        duplicate_check_cols = table_config.get(consts.DUPLICATE_CHECK_COLS) or []
        if duplicate_check_cols:
            join_clause = " and ".join(map(lambda x: f't.{x} = s.{x}', duplicate_check_cols))
            duplicate_check_cols_str = ','.join([f's.{x}' for x in duplicate_check_cols])
            query = f""" select {duplicate_check_cols_str} from {transformed_view.get(consts.ID)} s join
                                    {transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)} t
                                    ON  {join_clause} limit 1
                              """
            results = bigquery_client.query(query).result().to_dataframe()
            res = results.to_string(index=False)
            if not results.empty:
                raise AirflowFailException(
                    f"Duplicate entry with these details {res} exists for {segment_name} in "
                    f"source : '{transformed_view.get(consts.ID)}' ")

    def build_segment_loading_task(self, bigquery_config: dict, transformation_config: dict, segment_name: str):
        return PythonOperator(
            task_id="load_into_bq",
            python_callable=self.build_segment_loading_job,
            op_kwargs={'bigquery_config': bigquery_config,
                       'transformation_config': transformation_config,
                       'segment_name': segment_name}
        )

    def check_record_count(self, bigquery_config: dict, segment_name: str, **context):
        table_config = bigquery_config.get(consts.TABLES).get(segment_name)
        record_count_column = table_config.get(consts.RECORD_COUNT_COLUMN)
        if not self.is_trailer(segment_name) and record_count_column:
            tsys_count_str = context['ti'].xcom_pull(task_ids=f"{consts.TRAILER_SEGMENT_NAME}.rec_count_validate",
                                                     key=f"{consts.RECORD_COUNT}")
            tsys_count_json = json.loads(tsys_count_str)
            tsys_count = tsys_count_json.get(record_count_column).get('0')
            if tsys_count > 0:
                return f'{segment_name}.parse_file'
            else:
                return f'{segment_name}.rec_count_validate'
        else:
            return f'{segment_name}.parse_file'

    def build_record_count_check(self, bigquery_config: dict, segment_name: str):
        return BranchPythonOperator(
            task_id="record_count_check",
            python_callable=self.check_record_count,
            op_kwargs={
                'bigquery_config': bigquery_config,
                'segment_name': segment_name}
        )

    def build_transformation_config(self, bigquery_config: dict, output_dir: str, segment_name: str):

        table_config = bigquery_config.get(consts.TABLES).get(segment_name)

        parquet_files_path = f"{output_dir}/{segment_name}/*.parquet"

        bq_project_name = bigquery_config.get(consts.PROJECT_ID) or self.gcp_config[consts.LANDING_ZONE_PROJECT_ID]
        bq_processing_project_name = self.gcp_config[consts.PROCESSING_ZONE_PROJECT_ID]
        bq_dataset_name = bigquery_config.get(consts.DATASET_ID)
        bq_table_name = table_config[consts.TABLE_NAME]

        bq_ext_table_id = f"{bq_processing_project_name}.{bq_dataset_name}.{bq_table_name}_{consts.EXTERNAL_TABLE_SUFFIX}"
        bq_table_id = f"{bq_project_name}.{bq_dataset_name}.{bq_table_name}"

        additional_columns = table_config.get(consts.ADD_COLUMNS) or []
        exclude_columns = table_config.get(consts.DROP_COLUMNS) or []
        return {
            consts.EXTERNAL_TABLE_ID: bq_ext_table_id,
            consts.DATA_FILE_LOCATION: parquet_files_path,
            consts.ADD_COLUMNS: additional_columns,
            consts.DROP_COLUMNS: exclude_columns,
            consts.DESTINATION_TABLE: {
                consts.ID: bq_table_id
            }
        }

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
            parsing_task = self.build_segment_parsing_task(cluster_name, spark_config, parsing_job_config,
                                                           segment_config)
            transformation_config = self.build_transformation_config(bigquery_config, output_dir, segment_name)

            loading_task = self.build_segment_loading_task(bigquery_config, transformation_config, segment_name)
            validation_task = self.validate_rec_count_task(bigquery_config,
                                                           transformation_config.get(
                                                               consts.EXTERNAL_TABLE_ID),
                                                           segment_name)
            check_record_count_task = self.build_record_count_check(bigquery_config, segment_name)

            check_record_count_task >> parsing_task >> loading_task >> validation_task
            check_record_count_task >> validation_task
        return segment_task_group

    def get_body_segment_list(self, dag_config: dict):
        spark_config = dag_config.get(consts.SPARK)
        segment_configs = spark_config.get(consts.SEGMENT_ARGS)

        return [segment_name for segment_name in segment_configs if
                (segment_name != consts.TRAILER_SEGMENT_NAME and segment_name != 'AM0A')]

    def build_control_record_saving_job(self, file_name: str, output_dir: str, **context):
        job_params_str = json.dumps({
            'source_filename': file_name,
            'extract_path': output_dir
        })
        save_job_to_control_table(job_params_str, **context)

    def build_file_staging_task(self, dag_config: dict):
        file_prefix = dag_config.get(consts.FILE_PREFIX)
        return GCSToGCSOperator(
            task_id=f"staging_{file_prefix}_file",
            gcp_conn_id=self.gcp_config.get(consts.LANDING_ZONE_CONNECTION_ID),
            source_bucket="{{ dag_run.conf['bucket'] }}",
            source_object="{{ dag_run.conf['name'] }}",
            destination_bucket=dag_config.get(consts.PROCESSING_BUCKET),
            destination_object="{{ dag_run.conf['name']}}"
        )

    def get_file_create_dt(self, dag_id: str, dag_config: dict, **context):
        bigquery_client = bigquery.Client()
        bigquery_config = dag_config.get(consts.BIGQUERY)
        bq_processing_project_name = bigquery_config.get(consts.PROJECT_ID)
        bq_dataset_name = bigquery_config.get(consts.DATASET_ID)
        create_date_column = dag_config.get(consts.CREATE_DATE_COLUMN)
        bq_table = bigquery_config.get("tables").get("TRLR").get("table_name")

        create_date_sql = f"""
            SELECT MAX({create_date_column}) AS {create_date_column}
            FROM {bq_processing_project_name}.{bq_dataset_name}.{bq_table}
        """
        logging.info(f"Extracting file_create_dt using SQL: {create_date_sql}")
        create_dt_result = bigquery_client.query(create_date_sql).result().to_dataframe()
        file_create_dt = create_dt_result[create_date_column].values[0]

        if file_create_dt:
            logging.info(f"File create date for this execution is {file_create_dt}")
            context['ti'].xcom_push(key='file_create_dt', value=str(file_create_dt))
        else:
            raise AirflowException(f"file_create_dt is not found for {dag_id}")

    def trigger_dag(self, dag_id: str, dag_config: dict):
        return TriggerDagRunOperator(
            task_id=f"trigger_{dag_config.get('trigger_dag_id')}",
            trigger_dag_id=self.job_config.get(dag_id).get('trigger_dag_id'),
            conf={
                "file_create_dt": "{{ ti.xcom_pull(task_ids='get_file_date', key='file_create_dt') }}"
            },
            wait_for_completion=False,
            trigger_rule='all_success',
            retries=0,
            poke_interval=60,
        )

    def build_get_file_create_date_task(self, dag_id: str, dag_config: dict, **context):
        return PythonOperator(
            task_id='get_file_date',
            python_callable=self.get_file_create_dt,
            op_kwargs={'dag_id': dag_id, 'dag_config': dag_config}
        )

    def build_postprocessing_task_group(self, dag_id: str, dag_config: dict):
        outputpath = f"gs://{dag_config.get('processing_bucket_extract')}/{consts.STAGING_FOLDER_NAME}/{{{{ ti.xcom_pull(task_ids='FileStaging.get_latest_file', key='filename') }}}}" + '_extract'
        return PythonOperator(
            task_id='save_job_to_control_table',
            trigger_rule='none_failed',
            python_callable=self.build_control_record_saving_job,
            op_kwargs={'dag_id': dag_id,
                       'file_name': "{{ ti.xcom_pull(task_ids='FileStaging.get_latest_file', key='filename')}}",
                       'output_dir': outputpath,
                       'tables_info': self.extract_tables_info(dag_config)}
        )

    def create_dag(self, dag_id: str, dag_config: dict) -> DAG:
        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            schedule=None,
            start_date=datetime(2023, 1, 1, tzinfo=self.local_tz),
            dagrun_timeout=timedelta(hours=5),
            max_active_runs=1,
            catchup=False,
            is_paused_upon_creation=True
        )

        with dag:
            if dag_config.get(consts.READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(TSYS_FILE_LOADER, self.deploy_env)
                pause_unpause_dag(dag, is_paused)

            job_size = dag_config.get(consts.DATAPROC_JOB_SIZE)
            cluster_name = get_cluster_name_for_dag(dag_id)

            file_staging_task = self.build_file_staging_task(dag_config)
            cluster_creating_task = self.build_cluster_creating_task(cluster_name, job_size)
            preprocess_task = self.build_preprocessing_task_group(dag_id, cluster_name, dag_config)

            trailer_segment_task = self.build_segment_task_group(cluster_name, dag_config,
                                                                 'TRLR')
            body_segment_tasks = self.build_body_task_groups(cluster_name, dag_config,
                                                             self.get_body_segment_list(dag_config))
            control_table_task = self.build_postprocessing_task_group(dag_id, dag_config)
            get_file_create_date_task = self.build_get_file_create_date_task(dag_id, dag_config)
            dag_trigger_task = self.trigger_dag(dag_id, dag_config)
            start = EmptyOperator(task_id='Start')
            end = EmptyOperator(task_id='End')
            start >> file_staging_task >> cluster_creating_task >> preprocess_task >> trailer_segment_task >> body_segment_tasks >> control_table_task >> get_file_create_date_task >> dag_trigger_task >> end

        return add_tags(dag)
