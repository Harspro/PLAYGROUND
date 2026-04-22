import json
import logging
from datetime import timedelta, datetime
from typing import Final

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

import util.constants as consts
from bigquery_loader.bigquery_loader_base import LoadingFrequency, INITIAL_DEFAULT_ARGS
from bigquery_loader.bigquery_deltaloader import BigQueryDeltaLoader
from util.bq_utils import (
    parse_join_config,
    schema_preserving_load
)
from util.deploy_utils import (
    read_pause_unpause_setting,
    pause_unpause_dag
)
from google.cloud import bigquery
from util.logging_utils import build_spark_logging_info
from util.miscutils import (
    read_yamlfile_env,
    save_job_to_control_table,
    get_cluster_name_for_dag
)
import copy
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)

BIGQUERY_DELTA_LOADER: Final = 'bigquery_delta_loader'


class BigQueryCustomQueryDeltaLoader(BigQueryDeltaLoader):

    def __init__(self, config_filename: str, loading_frequency: LoadingFrequency, config_dir: str = None):
        super().__init__(config_filename, loading_frequency, config_dir)
        self.dbconfig = read_yamlfile_env(f'{self.config_dir}/db_config.yaml', self.deploy_env)

    def get_staging_folder(self, config: dict, execution_id: str):
        gcs_config = config.get(consts.GCS)
        if not gcs_config:
            gcs_config = {
                consts.BUCKET: f"pcb-{self.deploy_env}-staging-extract",
                consts.STAGING_FOLDER: f"{config[consts.FETCH_SPARK_JOB].get(consts.DBNAME)}/custom-query-delta-loading/{config[consts.BIGQUERY].get(consts.TABLE_NAME)}/{execution_id}"
            }
        return f"gs://{gcs_config[consts.BUCKET]}/{gcs_config[consts.STAGING_FOLDER]}"

    def build_control_record_saving_job(self, start_date: str, end_date: str, **context):
        job_params_str = json.dumps({
            'start_date': start_date,
            'end_date': end_date
        })
        save_job_to_control_table(job_params_str, **context)

    def build_fetching_job(self, cluster_name: str, config: dict, execution_id: str):
        db_name = config[consts.FETCH_SPARK_JOB][consts.DBNAME]
        if db_name not in self.dbconfig.keys():
            raise AirflowFailException("DB Name not understood")

        spark_config = copy.deepcopy(self.dbconfig[db_name][consts.FETCH_SPARK_JOB])
        spark_job_args = copy.deepcopy(config[consts.FETCH_SPARK_JOB])
        spark_config.update(spark_job_args)

        query = spark_config[consts.ARGS][consts.PCB_CUSTOM_QUERY]
        spark_config[consts.ARGS][consts.PCB_CUSTOM_QUERY] = query.format(start_date=f"'{spark_config[consts.ARGS][consts.PCB_LOWER_BOUND]}'", end_date=f"'{spark_config[consts.ARGS][consts.PCB_UPPER_BOUND]}'")

        arg_list = [spark_config[consts.APPLICATION_CONFIG], self.get_staging_folder(config, execution_id)]

        if consts.ARGS in spark_config:
            for k, v in spark_config[consts.ARGS].items():
                arg_list.append(f'{k}={v}')

        if not any(consts.DB_READ_PARALLELISM in arg for arg in arg_list):
            arg_list.append(consts.DB_READ_PARALLELISM + '=1')

        if consts.PROPERTIES not in spark_config:
            spark_config[consts.PROPERTIES] = consts.DEFAULT_SPARK_SETTINGS

        arg_list = build_spark_logging_info(dag_id='{{dag.dag_id}}', default_args=self.default_args, arg_list=arg_list)
        spark_job_dict = {
            consts.REFERENCE: {consts.PROJECT_ID: self.dataproc_config.get(consts.PROJECT_ID)},
            consts.PLACEMENT: {consts.CLUSTER_NAME: cluster_name},
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: spark_config[consts.JAR_FILE_URIS],
                consts.MAIN_CLASS: spark_config[consts.MAIN_CLASS],
                consts.FILE_URIS: spark_config[consts.FILE_URIS],
                consts.ARGS: arg_list,
                consts.PROPERTIES: spark_config[consts.PROPERTIES]
            }
        }
        return spark_job_dict

    def build_transformation_config(self, bq_client, context):
        execution_id = context.get('execution_id')
        config = context.get(consts.CONFIG)
        bq_config = config[consts.BIGQUERY]
        parquet_files_path = f"{self.get_staging_folder(config, execution_id)}/*.parquet"

        bq_project_name = bq_config.get(consts.PROJECT_ID) or self.gcp_config[consts.LANDING_ZONE_PROJECT_ID]
        bq_proc_project_name = self.gcp_config[consts.PROCESSING_ZONE_PROJECT_ID]
        bq_table_name = bq_config[consts.TABLE_NAME]

        bq_ext_table_id = f"{bq_proc_project_name}.{bq_config[consts.DATASET_ID]}.{bq_table_name}_{consts.EXTERNAL_TABLE_SUFFIX}"
        bq_table_id = f"{bq_project_name}.{bq_config[consts.DATASET_ID]}.{bq_table_name}"

        partition_field = bq_config.get('partition_field')
        clustering_fields = bq_config.get('clustering_fields')
        logger.info(f'partition: {partition_field}, cluster: {clustering_fields}')
        partition_str = f"PARTITION BY {partition_field}" if partition_field else ''
        clustering_str = "CLUSTER BY " + ", ".join(clustering_fields) if clustering_fields else ''
        logger.info(f'partition: {partition_str}, cluster: {clustering_str}')

        additional_columns = bq_config.get(consts.ADD_COLUMNS) or []
        add_file_name_config = bq_config.get(consts.ADD_FILE_NAME_COLUMN)
        if add_file_name_config:
            additional_columns.append(f"'{parquet_files_path}' AS {add_file_name_config.get(consts.ALIAS)}")

        exclude_columns = bq_config.get(consts.DROP_COLUMNS)

        join_spec = parse_join_config(bq_client, bq_config.get(consts.JOIN), bq_table_id)

        return {
            consts.EXTERNAL_TABLE_ID: bq_ext_table_id,
            consts.DATA_FILE_LOCATION: parquet_files_path,
            consts.ADD_COLUMNS: additional_columns,
            consts.DROP_COLUMNS: exclude_columns,
            consts.DESTINATION_TABLE: {
                consts.PARTITION: partition_str,
                consts.CLUSTERING: clustering_str,
                consts.ID: bq_table_id
            },
            consts.JOIN_SPECIFICATION: join_spec
        }

    def build_loading_job(self, **context):
        bq_client = bigquery.Client()
        transformation_config = self.build_transformation_config(bq_client, context)
        transformed_view = self.apply_transformations(bq_client, transformation_config)
        data_load_type = context.get(consts.CONFIG, {}).get(consts.BIGQUERY, {}).get(consts.DATA_LOAD_TYPE, None)
        bq_table_id = transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)

        if data_load_type == consts.FULL_REFRESH:
            # Build CREATE OR REPLACE TABLE DDL statement
            create_ddl = f"""
                            CREATE OR REPLACE TABLE
                                `{bq_table_id}`
                                {transformation_config.get(consts.DESTINATION_TABLE).get(consts.PARTITION)}
                                {transformation_config.get(consts.DESTINATION_TABLE).get(consts.CLUSTERING)}
                                AS
                                SELECT {transformed_view.get(consts.COLUMNS)}
                                FROM {transformed_view.get(consts.ID)};
                        """

            # Build INSERT statement
            insert_stmt = f"""
                        INSERT INTO `{bq_table_id}`
                        SELECT {transformed_view.get(consts.COLUMNS)}
                        FROM {transformed_view.get(consts.ID)};
                    """
            # The util function will choose TRUNCATE + INSERT or CREATE OR REPLACE based on table existence
            schema_preserving_load(create_ddl, insert_stmt, bq_table_id, bq_client=bq_client)
        else:
            loading_sql = f"""
                        CREATE TABLE IF NOT EXISTS
                            `{bq_table_id}`
                        {transformation_config.get(consts.DESTINATION_TABLE).get(consts.PARTITION)}
                        {transformation_config.get(consts.DESTINATION_TABLE).get(consts.CLUSTERING)}
                        AS
                            SELECT {transformed_view.get(consts.COLUMNS)}
                            FROM {transformed_view.get(consts.ID)}
                            LIMIT 0;
                        INSERT INTO `{bq_table_id}`
                        SELECT {transformed_view.get(consts.COLUMNS)}
                        FROM {transformed_view.get(consts.ID)};
                    """
            logger.info(loading_sql)
            bq_client.query_and_wait(loading_sql)

    def build_postprocessing_task_group(self, dag_id: str, config: dict, fromdt: str, todt: str):
        return PythonOperator(
            task_id='save_job_to_control_table',
            python_callable=self.build_control_record_saving_job,
            op_kwargs={'dag_id': dag_id,
                       'start_date': fromdt,
                       'end_date': todt,
                       'tables_info': self.extract_tables_info(config)
                       }
        )

    def create_dag(self, dag_id: str, config: dict) -> DAG:
        self.default_args.update(config.get(consts.DEFAULT_ARGS, INITIAL_DEFAULT_ARGS))
        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            schedule=self.get_schedule_interval(config),
            start_date=datetime(2024, 5, 5, tzinfo=self.local_tz),
            catchup=False,
            dagrun_timeout=timedelta(hours=24),
            max_active_runs=1,
            is_paused_upon_creation=True
        )

        with dag:
            if config.get('batch') and config['batch'].get('read_pause_deploy_config'):
                is_paused = read_pause_unpause_setting(BIGQUERY_DELTA_LOADER, self.deploy_env)
                pause_unpause_dag(dag, is_paused)

            is_sqlserver = config.get(consts.IS_SQLSERVER, None)
            start_point = EmptyOperator(task_id='start')
            end_point = EmptyOperator(task_id='end')
            job_size = config.get(consts.DATAPROC_JOB_SIZE)
            cluster_name = get_cluster_name_for_dag(dag_id)

            cluster_creating_task = self.build_cluster_creating_task(cluster_name, job_size, is_sqlserver)
            last_executation_task = self.validate_chunk_load_task(dag_id, config)
            fromdt = "{{ ti.xcom_pull(task_ids='check_last_executation', key='load_data_from') }}"
            todt = "{{ ti.xcom_pull(task_ids='check_last_executation', key='load_data_to') }}"
            lowerbound = str(fromdt)
            upperbound = str(todt)

            config[consts.FETCH_SPARK_JOB][consts.ARGS][consts.PCB_LOWER_BOUND] = lowerbound
            config[consts.FETCH_SPARK_JOB][consts.ARGS][consts.PCB_UPPER_BOUND] = upperbound

            execution_id = "{{ ti.xcom_pull(task_ids='check_last_executation', key='execution_id') }}"
            fetching_data_task = self.build_fetching_data_task(cluster_name, config, execution_id)

            loading_task = self.build_bq_load_task(dag_id, config, execution_id)
            control_table_task = self.build_postprocessing_task_group(dag_id, config, fromdt, todt)
            start_point >> last_executation_task >> cluster_creating_task >> fetching_data_task >> loading_task >> control_table_task >> end_point
            last_executation_task >> control_table_task

        return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}

        if not self.job_config:
            logger.info(f'Config file {self.config_file_path} is empty.')
            return dags

        for job_id, config in self.job_config.items():
            dags[job_id] = self.create_dag(job_id, config)

        return dags
