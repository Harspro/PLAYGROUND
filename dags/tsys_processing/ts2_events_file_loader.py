from datetime import timedelta, datetime

import util.constants as consts
import logging
from airflow.exceptions import AirflowException
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from typing import Final
from airflow.utils.task_group import TaskGroup
from google.cloud import bigquery
from tsys_processing.tsys_file_loader_base import TsysFileLoader
from util.bq_utils import (
    create_external_table,
    apply_column_transformation,
    apply_timestamp_transformation,
    apply_schema_sync_transformation,
    table_exists)
from util.deploy_utils import (
    read_pause_unpause_setting,
    pause_unpause_dag
)
from util.miscutils import (
    get_cluster_name_for_dag)
from dag_factory.terminus_dag_factory import add_tags

EVENTS_FILE_LOADER: Final = 'events_file_loader'
logger = logging.getLogger(__name__)


class Ts2EventsFileLoader(TsysFileLoader):
    def __init__(self, config_filename: str, config_dir: str = None):
        super().__init__(config_filename, config_dir)

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
        transformed_data = apply_timestamp_transformation(bigquery_client, transformed_data)
        if table_exists(bigquery_client, target_table_id):
            transformed_data = apply_schema_sync_transformation(bigquery_client, transformed_data, target_table_id)

        return transformed_data

    def build_preprocessing_task_group(self, dag_id: str, cluster_name: str, dag_config: dict):
        return self.build_filtering_task(cluster_name, dag_config)

    def build_segment_loading_job(self, bigquery_config: dict, transformation_config: dict):
        bigquery_client = bigquery.Client()
        transformed_view = self.apply_transformations(bigquery_client, transformation_config)

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

    def build_segment_loading_task(self, bigquery_config: dict, transformation_config: dict):
        return PythonOperator(
            task_id="load_into_bq",
            python_callable=self.build_segment_loading_job,
            op_kwargs={'bigquery_config': bigquery_config,
                       'transformation_config': transformation_config}
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
            parsing_task = self.build_segment_parsing_task(cluster_name, spark_config, parsing_job_config, segment_config)
            transformation_config = self.build_transformation_config(bigquery_config, output_dir, segment_name)

            loading_task = self.build_segment_loading_task(bigquery_config, transformation_config)

            parsing_task >> loading_task
            return segment_task_group

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

    def get_file_create_dt(self, dag_id: str, dag_config: dict, **context):
        bigquery_client = bigquery.Client()
        bigquery_config = dag_config.get(consts.BIGQUERY)
        bq_processing_project_name = bigquery_config.get(consts.PROJECT_ID)
        bq_dataset_name = bigquery_config.get(consts.DATASET_ID)
        create_date_column = bigquery_config.get(consts.CREATE_DATE_COLUMN)
        bq_table = bigquery_config.get(consts.TABLES).get("ALL_SEGMENT").get(consts.TABLE_NAME)

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

            body_segment_tasks = self.build_body_task_groups(cluster_name, dag_config,
                                                             self.get_body_segment_list(dag_config))
            control_table_task = self.build_postprocessing_task_group(dag_id, dag_config)
            start = EmptyOperator(task_id='Start')
            end = EmptyOperator(task_id='End')
            task = start >> file_staging_task >> cluster_creating_task >> \
                preprocess_task >> body_segment_tasks >> control_table_task

            if consts.KAFKA_WRITER in dag_config:
                dag_trigger_task = TriggerDagRunOperator(
                    task_id=f"{consts.KAFKA_WRITER_TASK}",
                    trigger_dag_id=dag_config[consts.KAFKA_WRITER].get(consts.DAG_ID),
                    logical_date=datetime.now(self.local_tz),
                    conf={
                        consts.BUCKET: dag_config.get(consts.PROCESSING_BUCKET_EXTRACT),
                        consts.NAME: "{{ dag_run.conf['name'] }}",
                        consts.FOLDER_NAME: "{{ dag_run.conf['folder_name'] }}",
                        consts.FILE_NAME: "{{ dag_run.conf['file_name'] }}",
                        consts.CLUSTER_NAME: cluster_name
                    },
                    wait_for_completion=True,
                    poke_interval=30
                )

                trigger = task >> dag_trigger_task
            else:
                trigger = task

            if consts.ORACLE_WRITER in dag_config:
                get_file_create_date_task = PythonOperator(
                    task_id='get_file_date',
                    python_callable=self.get_file_create_dt,
                    op_kwargs={
                        'dag_id': dag_id,
                        'dag_config': dag_config
                    }
                )
                oracle_dag_trigger_task = TriggerDagRunOperator(
                    task_id=f"trigger_{dag_config.get(consts.ORACLE_WRITER).get('dag_id')}",
                    trigger_dag_id=dag_config[consts.ORACLE_WRITER].get(consts.DAG_ID),
                    conf={
                        "file_create_dt": "{{ ti.xcom_pull(task_ids='get_file_date', key='file_create_dt') }}"
                    },
                    wait_for_completion=False
                )
                trigger >> get_file_create_date_task >> oracle_dag_trigger_task >> end
            else:
                trigger >> end

        return add_tags(dag)
