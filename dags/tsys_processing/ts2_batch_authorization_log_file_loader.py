import pendulum
from datetime import timedelta, datetime
import json
import util.constants as consts
import logging
from airflow.exceptions import AirflowException
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from typing import Final, Union
from airflow.utils.task_group import TaskGroup
from google.cloud import bigquery
from tsys_processing.tsys_file_loader_base import TsysFileLoader
from util.bq_utils import (
    create_external_table,
    apply_column_transformation,
    apply_timestamp_transformation,
    apply_schema_sync_transformation,
    table_exists,
    get_table_columns)
from util.deploy_utils import (
    read_pause_unpause_setting,
    pause_unpause_dag
)
from util.miscutils import (
    get_cluster_name_for_dag)
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)


class Ts2BatchAuthLogFileLoader(TsysFileLoader):
    def __init__(self, config_filename: str, config_dir: str = None):
        super().__init__(config_filename, config_dir)

    def apply_transformations(self, bigquery_client, transformation_config: dict):
        logger.info(transformation_config)
        transformed_data = create_external_table(bigquery_client, transformation_config.get(consts.EXTERNAL_TABLE_ID),
                                                 transformation_config.get(consts.DATA_FILE_LOCATION))
        add_columns = transformation_config.get(consts.ADD_COLUMNS)
        drop_columns = transformation_config.get(consts.DROP_COLUMNS)

        if add_columns or drop_columns:
            column_transform_view = apply_column_transformation(bigquery_client, transformed_data, add_columns,
                                                                drop_columns)
            transformed_data = column_transform_view
        target_table_id = transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)
        transformed_data = apply_timestamp_transformation(bigquery_client, transformed_data)
        if table_exists(bigquery_client, target_table_id):
            transformed_data = apply_schema_sync_transformation(bigquery_client, transformed_data, target_table_id)

        return transformed_data

    def build_preprocessing_task_group(self, dag_id: str, cluster_name: str, dag_config: dict):
        return self.build_filtering_task(cluster_name, dag_config)

    def build_segment_loading_job(self, bigquery_config: dict, transformation_config: dict, segment_name, **context):
        bigquery_client = bigquery.Client()
        transformed_view = self.apply_transformations(bigquery_client, transformation_config)

        context['ti'].xcom_push(key=segment_name, value=f'{transformed_view}')

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
            raise AirflowException("data_load_type is not APPEND_ONLY")
        logger.info(loading_sql)
        bigquery_client.query(loading_sql).result()

    def build_segment_loading_task(self, bigquery_config: dict, transformation_config: dict, segment_name):
        return PythonOperator(
            task_id="load_into_bq",
            python_callable=self.build_segment_loading_job,
            op_kwargs={'bigquery_config': bigquery_config,
                       'transformation_config': transformation_config,
                       'segment_name': segment_name}
        )

    def load_into_parent_table(self, bigquery_config, segment_name, **context):
        bigquery_client = bigquery.Client()
        parent_table = bigquery_config.get(consts.PARENT_TABLE)
        logger.info(parent_table)
        table_config = bigquery_config.get(consts.TABLES).get(segment_name)
        child_table = table_config[consts.TABLE_NAME]

        bq_project_name = bigquery_config.get(consts.PROJECT_ID) or self.gcp_config[consts.LANDING_ZONE_PROJECT_ID]
        bq_dataset_name = bigquery_config.get(consts.DATASET_ID)

        parent_table_id = f"{bq_project_name}.{bq_dataset_name}.{parent_table}"
        child_table_id = f"{bq_project_name}.{bq_dataset_name}.{child_table}"

        column_names = get_table_columns(bigquery_client, child_table_id)['columns']

        transformed_view = (context['ti'].xcom_pull(task_ids=f'{segment_name}.load_into_bq', key=segment_name))
        transformed_view = transformed_view.replace("'", "\"")
        transformed_view = json.loads(transformed_view)
        logger.info(type(transformed_view))

        parent_loading_sql = f"""
               INSERT INTO `{parent_table_id}`  ({column_names})
        SELECT
        {transformed_view.get(consts.COLUMNS)}
        FROM
        {transformed_view.get(consts.ID)}
        ;
           """
        logger.info(parent_loading_sql)
        bigquery_client.query(parent_loading_sql).result()

    def load_parent_from_child(self, bigquery_config: dict, segment_name):
        return PythonOperator(
            task_id=segment_name + "_load_into_parent",
            python_callable=self.load_into_parent_table,
            op_kwargs={'bigquery_config': bigquery_config,
                       'segment_name': segment_name}
        )

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

            parsing_task >> loading_task
            return segment_task_group

    def build_load_into_parent_tasks(self, dag_config: dict, body_segments: Union[list, set]):
        task_groups = []

        for segment_name in body_segments:
            bigquery_config = dag_config.get(consts.BIGQUERY)
            task_groups.append(self.load_parent_from_child(bigquery_config, segment_name))

        return task_groups

    def validate_segment_count(self, dag_config, body_segments):
        segment_count = 0
        num = 0
        tsys_count = 0

        bigquery_client = bigquery.Client()
        bigquery_config = dag_config.get(consts.BIGQUERY)
        bq_processing_project_name = self.gcp_config[consts.PROCESSING_ZONE_PROJECT_ID]
        bq_dataset_name = bigquery_config.get(consts.DATASET_ID)

        for segment_name in body_segments:
            table_config = bigquery_config.get(consts.TABLES).get(segment_name)
            bq_table_name = table_config[consts.TABLE_NAME]

            bq_ext_table_id = f"{bq_processing_project_name}.{bq_dataset_name}.{bq_table_name}_{consts.EXTERNAL_TABLE_SUFFIX}"
            query = f"""SELECT COUNT(*) as record_count FROM {bq_ext_table_id}"""
            logger.info(query)
            result = bigquery_client.query(query).result().to_dataframe()
            segment_count = result['record_count'].values[0]

            segment_count = int(segment_count)
            logger.info(f"""Record count for {segment_name} - {segment_count}""")

            num = num + segment_count

        table_config = bigquery_config.get(consts.TABLES).get("TRLR")
        bq_table_name = table_config[consts.TABLE_NAME]

        bq_table_id_trlr = f"{bq_processing_project_name}.{bq_dataset_name}.{bq_table_name}_{consts.EXTERNAL_TABLE_SUFFIX}"
        record_count_column = table_config.get(consts.RECORD_COUNT_COLUMN)
        logger.info("########## checking record count value in trailer #######")
        query_trlr = f"""
                SELECT `{record_count_column}` as tsys_count FROM {bq_table_id_trlr}
                """

        logger.info(query_trlr)
        result = bigquery_client.query(query_trlr).result().to_dataframe()
        tsys_count = result['tsys_count'].values[0]
        tsys_count = int(tsys_count)

        logger.info(f"""Record count for Trailer - {tsys_count}""")

        if int(tsys_count) != int(num):
            raise AirflowException(f"""tsys count {tsys_count} does not match bigquery count {num}""")

    def validate_count(self, dag_config, body_segments):
        return PythonOperator(
            task_id='validate_segment_record_count',
            python_callable=self.validate_segment_count,
            op_kwargs={'dag_config': dag_config,
                       'body_segments': body_segments}
        )

    def build_postprocessing_task_group(self, dag_id: str, dag_config: dict):
        outputpath = self.get_output_dir_path(dag_config)
        return PythonOperator(
            task_id='save_job_to_control_table',
            trigger_rule='none_failed',
            python_callable=self.build_control_record_saving_job,
            op_kwargs={'dag_id': dag_id,
                       'file_name': "{{ dag_run.conf['name']}}",
                       'output_dir': outputpath,
                       'tables_info': self.extract_tables_info(dag_config)}
        )

    # this step is for triggering Convergence DAG (mobile_token_regular_segment_kafka_writer)
    def dag_trigger_function_for_mobile_token_regular_segment_kafka_writer(self, dag_config: dict):
        return TriggerDagRunOperator(
            task_id="trigger_mobile_token_regular_segment_kafka_writer",
            trigger_dag_id=dag_config[consts.TRIGGER_CONFIG].get('dag_to_trigger'),
            wait_for_completion=dag_config[consts.TRIGGER_CONFIG].get(consts.WAIT_FOR_COMPLETION),  # set it to False for asynchronous call
            poke_interval=dag_config[consts.TRIGGER_CONFIG].get(consts.POKE_INTERVAL),
            logical_date=datetime.now(tz=pendulum.timezone('America/Toronto'))
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

        with (dag):
            if dag_config.get(consts.READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(dag_id, self.deploy_env)
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
            validate_rec_count = self.validate_count(dag_config, self.get_body_segment_list(dag_config))
            load_parent_table_task = self.build_load_into_parent_tasks(dag_config,
                                                                       self.get_body_segment_list(dag_config))
            control_table_task = self.build_postprocessing_task_group(dag_id, dag_config)
            trigger_mobile_token_regular_segment_kafka_writer_task = self.dag_trigger_function_for_mobile_token_regular_segment_kafka_writer(dag_config)
            start = EmptyOperator(task_id='Start')
            end = EmptyOperator(task_id='End')

            start >> file_staging_task >> cluster_creating_task >> preprocess_task >> \
                trailer_segment_task >> body_segment_tasks >> validate_rec_count >> \
                load_parent_table_task >> control_table_task >> \
                trigger_mobile_token_regular_segment_kafka_writer_task >> end

        return add_tags(dag)
