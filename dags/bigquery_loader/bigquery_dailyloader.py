import json
import logging
from copy import deepcopy
from datetime import timedelta, datetime
from typing import Final

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator
)

import util.constants as consts
from bigquery_loader.bigquery_loader_base import BigQueryLoader, LoadingFrequency, INITIAL_DEFAULT_ARGS


from util.miscutils import (
    read_yamlfile_env,
    save_job_to_control_table,
    get_dagrun_last,
    get_cluster_config_by_job_size,
    get_cluster_name_for_dag
)
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)

BIGQUERY_DAILY_LOADER: Final = 'bigquery_daily_loader'
TRIGGER_DAG_ID: Final = 'trigger_dag_id'


class BigQueryDailyLoader(BigQueryLoader):

    def __init__(self, config_filename: str, loading_frequency: LoadingFrequency, config_dir: str = None):
        super().__init__(config_filename, loading_frequency, config_dir)
        self.dbconfig = read_yamlfile_env(f'{self.config_dir}/db_config.yaml', self.deploy_env)

    def validate_daily_load_job(self, dag_id: str, config: dict, **context):
        last_run_date = ""
        bq_results = get_dagrun_last(dag_id)
        if bq_results.total_rows != 0:
            results = bq_results.to_dataframe()
            job_params = json.loads(results['job_params'].values[0])
            last_run_date = str(job_params['last_run_date'])
            logger.info(f"last_run_date:, {last_run_date}")
        curr_date = datetime.now(self.local_tz)
        curr_run_date = curr_date.strftime("%Y-%m-%d")
        logger.info(f"curr_run_date:, {curr_run_date}")

        context['ti'].xcom_push(key='load_date', value=f'{curr_run_date}')

        config[consts.DAG_ID] = dag_id
        if config.get(consts.DAG, {}).get('is_multiple_runs', None) or last_run_date != curr_run_date:
            return consts.CLUSTER_CREATING_TASK_ID
        else:
            return "save_job_to_control_table"

    def validate_daily_load_task(self, dag_id: str, config: dict):
        return BranchPythonOperator(
            task_id='check_last_executation',
            python_callable=self.validate_daily_load_job,
            op_kwargs={
                consts.DAG_ID: dag_id,
                consts.CONFIG: config
            }
        )

    def build_control_record_saving_job(self, last_run_date: str, **context):
        job_params_str = json.dumps({
            'last_run_date': last_run_date
        })
        save_job_to_control_table(job_params_str, **context)

    def build_postprocessing_task_group(self, dag_id: str, config: dict):
        return PythonOperator(
            task_id='save_job_to_control_table',
            python_callable=self.build_control_record_saving_job,
            op_kwargs={
                consts.DAG_ID: dag_id,
                'last_run_date': "{{ ti.xcom_pull(task_ids='check_last_executation', key='load_date') }}",
                'tables_info': self.extract_tables_info(config)
            }
        )

    def create_dag(self, dag_id: str, config: dict) -> DAG:
        default_args = config.get(consts.DEFAULT_ARGS, INITIAL_DEFAULT_ARGS)
        dag = DAG(
            dag_id=dag_id,
            default_args=default_args,
            schedule=self.get_schedule_interval(config),
            start_date=datetime(2025, 1, 1, tzinfo=self.local_tz),
            catchup=False,
            dagrun_timeout=timedelta(hours=24),
            max_active_runs=1,
            is_paused_upon_creation=True,
            tags=config.get(consts.TAGS, [])
        )

        with dag:
            is_sqlserver = config.get(consts.IS_SQLSERVER, None)
            start_point = EmptyOperator(task_id=consts.START_TASK_ID)

            end_point = EmptyOperator(task_id=consts.END_TASK_ID)

            job_size = config.get(consts.DATAPROC_JOB_SIZE)
            cluster_name = get_cluster_name_for_dag(dag_id)

            cluster_creating_task = DataprocCreateClusterOperator(
                task_id=consts.CLUSTER_CREATING_TASK_ID,
                project_id=self.dataproc_config.get(consts.PROJECT_ID),
                cluster_config=get_cluster_config_by_job_size(self.deploy_env, self.gcp_config.get(consts.NETWORK_TAG), job_size, is_sqlserver),
                region=self.dataproc_config.get(consts.LOCATION),
                cluster_name=cluster_name
            )

            last_executation_task = self.validate_daily_load_task(dag_id, config)

            fetching_data_task = DataprocSubmitJobOperator(
                task_id="fetch_data_task",
                job=self.build_fetching_job(cluster_name, config),
                region=self.dataproc_config.get(consts.LOCATION),
                project_id=self.dataproc_config.get(consts.PROJECT_ID),
                gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID)
            )

            loading_task = PythonOperator(
                task_id="load_data_task",
                python_callable=self.build_loading_job,
                op_kwargs={
                    consts.CONFIG: config,
                    consts.LOADING_FREQUENCY: self.loading_frequency
                },
            )

            control_table_task = self.build_postprocessing_task_group(dag_id, config)

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
