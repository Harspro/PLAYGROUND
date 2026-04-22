import logging
from datetime import timedelta, datetime
from typing import Final

from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator

import util.constants as consts
from bigquery_loader.bigquery_loader_base import LoadingFrequency, INITIAL_DEFAULT_ARGS
from bigquery_loader.bigquery_custom_query_deltaloader import BigQueryCustomQueryDeltaLoader
from util.deploy_utils import (
    read_pause_unpause_setting,
    pause_unpause_dag
)
from util.miscutils import (
    read_yamlfile_env,
    save_job_to_control_table,
    get_cluster_name_for_dag
)
from google.cloud import bigquery
from util.bq_utils import (
    run_bq_query
)
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)

BIGQUERY_DELTA_LOADER: Final = 'bigquery_delta_loader'
IDEMPOTENT: Final = 'idempotent'
DATA_MERGE_CONFIG: Final = 'data_merge_config'
MERGE_SCRIPT_LOCATION: Final = 'merge_script_location'
CLEANUP_SCRIPT_LOCATION: Final = 'cleanup_script_location'
CUSTOM_QUERY_FLATTEN_LOAD: Final = 'custom-query-flatten-and-loading'
QUERY_PARAM: Final = 'pcb.bigquery.loader.source.query'
BULK_LOAD_DATA_AGGREGATION_CONFIG: Final = 'bulk_load_data_aggregation_config'
DATA_AGGREGATION_SCRIPT_LOCATION: Final = 'script_location'
MODE_BULK: Final = 'bulk'
MODE_SINGLE: Final = 'single'

SINGLE_RECORD_LOAD_DATA_AGGREGATION_CONFIG: Final = 'single_record_load_data_aggregation_config'
SINGLE_RECORD_DAG_ID_SUFFIX: Final = '_single_record'

GENERATE_REPORT_DAG: Final = 'genereate_report'
ARGS: Final = 'args'


class BigQueryDataFalttenerAndLoader(BigQueryCustomQueryDeltaLoader):

    def __init__(self, config_filename: str, loading_frequency: LoadingFrequency, config_dir: str = None):
        super().__init__(config_filename, loading_frequency, config_dir)
        self.dbconfig = read_yamlfile_env(f'{self.config_dir}/db_config.yaml', self.deploy_env)

    def get_staging_folder(self, config: dict, execution_id: str):
        gcs_config = config.get(consts.GCS)
        if not gcs_config:
            gcs_config = {
                consts.BUCKET: f"pcb-{self.deploy_env}-staging-extract",
                consts.STAGING_FOLDER: f"{config[consts.FETCH_SPARK_JOB].get(consts.DBNAME)}/{CUSTOM_QUERY_FLATTEN_LOAD}/{config[consts.BIGQUERY].get(consts.TABLE_NAME)}/{execution_id}"
            }
        return f"gs://{gcs_config[consts.BUCKET]}/{gcs_config[consts.STAGING_FOLDER]}"

    def build_loading_job(self, **context):
        bq_client = bigquery.Client()
        transformation_config = self.build_transformation_config(bq_client, context)
        self.apply_transformations(bq_client, transformation_config)
        config = context.get(consts.CONFIG)
        if config[DATA_MERGE_CONFIG].get(IDEMPOTENT) == 'Y':
            cleanup_file_path = f'{settings.DAGS_FOLDER}/{consts.CONFIG}/{config[DATA_MERGE_CONFIG].get(CLEANUP_SCRIPT_LOCATION)}'
            with open(cleanup_file_path) as file:
                cleanup_sql = file.read()
            cleanup_sql = cleanup_sql.replace(consts.ENV_PLACEHOLDER, self.deploy_env)
            run_bq_query(cleanup_sql)
        merge_file_path = f'{settings.DAGS_FOLDER}/{consts.CONFIG}/{config[DATA_MERGE_CONFIG].get(MERGE_SCRIPT_LOCATION)}'
        with open(merge_file_path) as file:
            merge_sql = file.read()
        merge_sql = merge_sql.replace(consts.ENV_PLACEHOLDER, self.deploy_env)
        run_bq_query(merge_sql)

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
            start_point = EmptyOperator(task_id='start')
            end_point = EmptyOperator(task_id='end')
            job_size = config.get(consts.DATAPROC_JOB_SIZE)
            cluster_name = get_cluster_name_for_dag(dag_id)
            cluster_creating_task = self.build_cluster_creating_task(cluster_name, job_size)
            fromdt = "{{ ti.xcom_pull(task_ids='check_last_executation', key='load_data_from') }}"
            todt = "{{ ti.xcom_pull(task_ids='check_last_executation', key='load_data_to') }}"
            lowerbound = str(fromdt)
            upperbound = str(todt)
            bounds = {consts.PCB_LOWER_BOUND: lowerbound, consts.PCB_UPPER_BOUND: upperbound}
            config[consts.FETCH_SPARK_JOB].update({ARGS: bounds})
            execution_id = "{{ ti.xcom_pull(task_ids='check_last_executation', key='execution_id') }}"
            fetching_data_task = self.build_fetching_data_task(cluster_name, config, execution_id)
            loading_task = self.build_bq_load_task(dag_id, config, execution_id)
            control_table_task = self.build_postprocessing_task_group(dag_id, config, fromdt, todt)
            start_point >> cluster_creating_task >> fetching_data_task >> loading_task >> control_table_task >> end_point

        return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}
        if not self.job_config:
            logger.info(f'Config file {self.config_file_path} is empty.')
            return dags
        for job_id, config in self.job_config.items():
            dags[job_id] = self.create_dag(job_id, config)
            if config[SINGLE_RECORD_LOAD_DATA_AGGREGATION_CONFIG] is not None:
                single_dag_id = f"{job_id}{SINGLE_RECORD_DAG_ID_SUFFIX}"
                config[SINGLE_RECORD_LOAD_DATA_AGGREGATION_CONFIG].update({MODE_SINGLE: 'Y'})
                dags[single_dag_id] = self.create_dag(single_dag_id, config)
            if config[GENERATE_REPORT_DAG] == "Y":
                logger.info('TODO - Generate report DAG')
        return dags

    def build_fetching_job(self, cluster_name: str, config: dict, execution_id: str):
        if config[SINGLE_RECORD_LOAD_DATA_AGGREGATION_CONFIG].get(MODE_SINGLE) is None:
            data_aggregation_script_path = f'{settings.DAGS_FOLDER}/{consts.CONFIG}/{config[BULK_LOAD_DATA_AGGREGATION_CONFIG].get(DATA_AGGREGATION_SCRIPT_LOCATION)}'
        else:
            data_aggregation_script_path = f'{settings.DAGS_FOLDER}/{consts.CONFIG}/{config[SINGLE_RECORD_LOAD_DATA_AGGREGATION_CONFIG].get(DATA_AGGREGATION_SCRIPT_LOCATION)}'
        with open(data_aggregation_script_path) as file:
            data_aggregation_sql = file.read()
        config[consts.FETCH_SPARK_JOB][consts.ARGS].update({QUERY_PARAM: f'{data_aggregation_sql}'})
        return super().build_fetching_job(cluster_name, config, execution_id)
