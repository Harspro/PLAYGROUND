import logging
from datetime import datetime, timedelta

import pendulum
from airflow import DAG, settings
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator
)

import util.constants as consts
from util.logging_utils import build_spark_logging_info
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env,
    get_cluster_name_for_dag,
    get_ephemeral_cluster_config
)
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)


class DatabaseReadinessTestTool:
    def __init__(self, config_filename: str, config_dir: str = None):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]

        self.dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/config'

        self.config_dir = config_dir
        self.job_config = read_yamlfile_env(f'{config_dir}/{config_filename}', self.deploy_env)
        self.dbconfig = read_yamlfile_env(f'{self.config_dir}/db_config.yaml', self.deploy_env)

        self.default_args = {
            "owner": "team-centaurs",
            'capability': 'TBD',
            'severity': 'P3',
            'sub_capability': 'TBD',
            'business_impact': 'TBD',
            'customer_impact': 'TBD',
            "email": [],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 10,
            "retry_delay": timedelta(minutes=1),
            "retry_exponential_backoff": True
        }

        self.local_tz = pendulum.timezone('America/Toronto')

    def build_fetching_job(self, staging_folder: str, cluster_name: str, config: dict):
        db_name = config[consts.FETCH_SPARK_JOB][consts.DBNAME]
        if db_name not in self.dbconfig.keys():
            raise AirflowFailException("DB Name not understood")
        spark_config = self.dbconfig[db_name][consts.FETCH_SPARK_JOB].copy()

        arg_list = [spark_config[consts.APPLICATION_CONFIG], staging_folder]

        for k, v in config[consts.FETCH_SPARK_JOB][consts.ARGS].items():
            arg_list.append(f'{k}={v}')

        arg_list = build_spark_logging_info(dag_id='{{dag.dag_id}}', default_args=self.default_args, arg_list=arg_list)

        return {
            consts.REFERENCE: {consts.PROJECT_ID: self.dataproc_config.get(consts.PROJECT_ID)},
            consts.PLACEMENT: {consts.CLUSTER_NAME: cluster_name},
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: spark_config[consts.JAR_FILE_URIS],
                consts.MAIN_CLASS: spark_config[consts.MAIN_CLASS],
                consts.FILE_URIS: spark_config[consts.FILE_URIS],
                consts.ARGS: arg_list,
                consts.PROPERTIES: consts.DEFAULT_SPARK_SETTINGS
            }
        }

    def create_dag(self, dag_id: str, config: dict) -> DAG:
        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            schedule=None,
            start_date=datetime(2024, 1, 1),
            dagrun_timeout=timedelta(hours=5),
            max_active_runs=1,
            catchup=False,
            is_paused_upon_creation=True
        )

        with dag:
            start_point = EmptyOperator(task_id='start')
            end_point = EmptyOperator(task_id='end')

            is_sqlserver = config.get('is_sqlserver')
            cluster_name = get_cluster_name_for_dag(dag_id)

            cluster_creating_task = DataprocCreateClusterOperator(
                task_id=consts.CLUSTER_CREATING_TASK_ID,
                project_id=self.dataproc_config.get(consts.PROJECT_ID),
                cluster_config=get_ephemeral_cluster_config(deploy_env=self.deploy_env,
                                                            network_tag=self.gcp_config.get(consts.NETWORK_TAG),
                                                            num_worker=2,
                                                            is_sqlserver=is_sqlserver),
                region=self.dataproc_config.get(consts.LOCATION),
                cluster_name=cluster_name
            )

            staging_folder = f"gs://pcb-{self.deploy_env}-staging-extract/db-readiness-test/{dag_id}"

            fetching_task = DataprocSubmitJobOperator(
                task_id='fetch_data',
                job=self.build_fetching_job(staging_folder, cluster_name, config),
                region=self.dataproc_config.get(consts.LOCATION),
                project_id=self.dataproc_config.get(consts.PROJECT_ID),
                gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID)
            )

            start_point >> cluster_creating_task >> fetching_task >> end_point

        return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}

        if self.job_config:
            for job_id, config in self.job_config.items():
                dags[job_id] = self.create_dag(job_id, config)

        return dags


globals().update(DatabaseReadinessTestTool(config_filename='database_readiness_test_config.yaml').create_dags())
