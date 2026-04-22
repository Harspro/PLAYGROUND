import logging
import pendulum
import util.constants as consts
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator
)
from datetime import datetime, date, timedelta
from typing import Final
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env,
    get_cluster_name,
    get_cluster_config_by_job_size
)
from util.deploy_utils import (
    read_pause_unpause_setting,
    pause_unpause_dag
)
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)

PCB_TABLE_COMPARATOR_PREFIX: Final = 'pcb.table.comparator'
JOB_HISTORY_PATH: Final = 'job_history_path'
TEST_CASES: Final = 'test-cases'
CASE_NAME: Final = 'case.name'
IS_ENABLED: Final = 'is.enabled'
WHERE_CONDITION = 'where.condition'
DATE_PATTERN = '%Y-%m-%d'


class TableComparator:
    def __init__(self, dag_id: str = None, config_filename: str = None, config_dir: str = None):

        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.current_date = date.today().strftime(DATE_PATTERN)
        self.previous_date = (date.today() - timedelta(days=1)).strftime(DATE_PATTERN)
        # Allow override for manual trigger
        self.conf_current_date = "{{ dag_run.conf['current_date'] if dag_run else "" }}"
        self.conf_previous_date = "{{ dag_run.conf['previous_date'] if dag_run else "" }}"

        if dag_id:
            self.DAG_ID = dag_id
        else:
            self.DAG_ID = consts.AUTOMATED_TESTING_DAG_ID

        if not self.conf_current_date:
            self.current_report_date = self.conf_current_date
        else:
            self.current_report_date = self.current_date

        if not self.conf_previous_date:
            self.previous_report_date = self.conf_previous_date
        else:
            self.previous_report_date = self.previous_date

        self.dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/config'

        if config_filename is None:
            config_filename = 'table_comparator_config.yaml'

        self.job_config = read_yamlfile_env(f'{config_dir}/{config_filename}', self.deploy_env) or {}

        self.schedule = None

        if consts.DAG in self.job_config:
            self.schedule = self.job_config.get(consts.DAG).get(consts.SCHEDULE_INTERVAL)

        if consts.DATAPROC_CLUSTER_NAME in self.job_config:
            self.cluster_name = self.job_config.get(consts.DATAPROC_CLUSTER_NAME)
        else:
            self.cluster_name = get_cluster_name(True, self.dataproc_config, dag_id=self.DAG_ID)

        self.job_size = self.job_config.get(consts.DATAPROC_JOB_SIZE)

        self.default_args = {
            'owner': 'team-centaurs',
            'capability': 'TBD',
            'severity': 'P3',
            'sub_capability': 'TBD',
            'business_impact': 'TBD',
            'customer_impact': 'TBD',
            'depends_on_past': False,
            "email": [],
            "email_on_failure": False,
            "email_on_retry": False
        }

        self.local_tz = pendulum.timezone(consts.TORONTO_TIMEZONE_ID)

    def create_task(self, test_case):
        spark_job_config = self.job_config[consts.SPARK_JOB]

        arg_list = [spark_job_config[consts.APPLICATION_CONFIG],
                    f'{PCB_TABLE_COMPARATOR_PREFIX}.job.history.path={spark_job_config[JOB_HISTORY_PATH]}']

        for k, v in test_case.items():
            arg_list.append(f'{PCB_TABLE_COMPARATOR_PREFIX}.{k}={v}')

        dag_info = f"[dag_name: {self.DAG_ID}, dag_run_id: {'{{run_id}}'}, dag_owner: {self.default_args['owner']}]"
        arg_list.append(f'{consts.SPARK_DAG_INFO}={dag_info}')
        spark_job = {
            consts.REFERENCE: {consts.PROJECT_ID: self.dataproc_config.get(consts.PROJECT_ID)},
            consts.PLACEMENT: {consts.CLUSTER_NAME: self.cluster_name},
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: spark_job_config[consts.JAR_FILE_URIS],
                consts.MAIN_CLASS: spark_job_config[consts.MAIN_CLASS],
                consts.FILE_URIS: spark_job_config[consts.FILE_URIS],
                consts.ARGS: arg_list
            }
        }

        test_task = DataprocSubmitJobOperator(
            task_id=test_case[CASE_NAME],
            job=spark_job,
            region=self.dataproc_config.get(consts.LOCATION),
            project_id=self.dataproc_config.get(consts.PROJECT_ID),
            gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID)
        )

        return test_task

    def create_dag(self, dag_id: str) -> DAG:
        self.default_args.update(self.job_config.get(consts.DEFAULT_ARGS, {}))

        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            schedule=self.schedule,
            start_date=datetime(2023, 1, 1, tzinfo=self.local_tz),
            catchup=False,
            max_active_runs=1,
            dagrun_timeout=timedelta(hours=5),
            is_paused_upon_creation=True,
            max_active_tasks=3,
            tags=self.job_config.get(consts.TAGS)
        )

        with dag:
            if self.job_config.get(consts.READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(dag_id, self.deploy_env)
                pause_unpause_dag(dag, is_paused)

            start_point = EmptyOperator(task_id='start')
            end_point = EmptyOperator(task_id='end')

            cluster_creating_task = DataprocCreateClusterOperator(
                task_id=consts.CLUSTER_CREATING_TASK_ID,
                project_id=self.dataproc_config.get(consts.PROJECT_ID),
                cluster_config=get_cluster_config_by_job_size(self.deploy_env, self.gcp_config.get(consts.NETWORK_TAG),
                                                              self.job_size),
                region=self.dataproc_config.get(consts.LOCATION),
                cluster_name=self.cluster_name
            )

            start_point >> cluster_creating_task

            test_tasks = []

            for test_case in self.job_config[TEST_CASES]:
                if test_case.get(IS_ENABLED):
                    if WHERE_CONDITION in test_case:
                        test_case[WHERE_CONDITION] = test_case[WHERE_CONDITION].replace("{current_date}", self.current_report_date)
                        test_case[WHERE_CONDITION] = test_case[WHERE_CONDITION].replace("{previous_date}", self.previous_report_date)

                    test_tasks.append(self.create_task(test_case))

                cluster_creating_task >> test_tasks >> end_point

        return add_tags(dag)

    def create(self) -> dict:
        if not self.job_config:
            logger.info('Config file is empty.')
            return {}

        logger.info(f"comparator config. type: {type(self.job_config)}, value: {self.job_config}")
        return {consts.DAG_ID: self.create_dag(self.DAG_ID)}
