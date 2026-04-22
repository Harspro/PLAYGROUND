# Standard Library Imports
import logging
from datetime import datetime, timedelta
from typing import Final, List
from copy import deepcopy

# Third-Party Imports
import pendulum

# Airflow Imports
from airflow import DAG, settings
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowFailException

# Internal/Project-Specific Imports
import util.constants as consts
from util.miscutils import read_file_env, read_variable_or_file, read_yamlfile_env
from bigquery_db_connector_loader.bigquery_db_connector_base import BigQueryDbConnector

logger = logging.getLogger(__name__)

"""
Initial default args to be used, users of the class can override the default args
as needed through configuration placed in the "".yaml file.
"""
INITIAL_DEFAULT_ARGS: Final = {
    'owner': 'TBD',
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


class BigQueryTaskExecutor:
    """Class to generate Airflow DAGs for BigQuery staging queries based on YAML config"""

    def __init__(self, config_filename: str, config_dir: str = None):
        """
        Constructor: sets up gcp/job configuration for class.

        :param config_filename: configuration file name.
        :type config_filename: str

        :param config_dir: configuration directory name, if none is provided will utilize
            the settings.DAGS_FOLDER.
        :type config_dir: str
        """

        self.default_args = deepcopy(INITIAL_DEFAULT_ARGS)
        self.local_tz = pendulum.timezone('America/Toronto')

        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/config/bigquery_task_executor_configs'

        self.config_dir = config_dir
        self.job_config = read_yamlfile_env(f'{config_dir}/{config_filename}', self.deploy_env)

    def create_dag(self, dag_id: str, config: dict) -> DAG:
        """Create and return an Airflow DAG based on config"""
        self.default_args.update(config.get(consts.DEFAULT_ARGS, INITIAL_DEFAULT_ARGS))
        dag_timeout = int(config[consts.DAG].get(consts.DAGRUN_TIMEOUT)) if config[consts.DAG].get(consts.DAGRUN_TIMEOUT) else 10080
        max_active_runs = config[consts.DAG].get(consts.MAX_ACTIVE_RUNS, 1)

        self.dag = DAG(
            dag_id=dag_id,
            schedule=config[consts.DAG].get(consts.SCHEDULE_INTERVAL),
            start_date=datetime(2025, 1, 1, tzinfo=self.local_tz),
            catchup=False,
            description=config[consts.DAG].get(consts.DESCRIPTION),
            default_args=self.default_args,
            dagrun_timeout=timedelta(minutes=dag_timeout),
            is_paused_upon_creation=True,
            tags=config[consts.DAG].get(consts.TAGS),
            max_active_runs=max_active_runs
        )

        tasks = self._create_bigquery_tasks(config=config)

        # Set sequential dependencies
        for i in range(len(tasks) - 1):
            tasks[i] >> tasks[i + 1]

        return self.dag

    def _create_bigquery_tasks(self, config: dict) -> List[PythonOperator]:
        """Create BigQuery staging tasks from config"""
        tasks = []

        # Empty Operator, used to indicate starting and ending tasks.
        start_point = EmptyOperator(task_id=consts.START_TASK_ID, dag=self.dag)
        end_point = EmptyOperator(task_id=consts.END_TASK_ID, dag=self.dag)

        env = self.deploy_env
        tasks.append(start_point)

        # Get the bigquery_tasks list from config
        bigquery_task_configs = config[consts.BIGQUERY_TASKS]

        # Process each task configuration in the list
        for task_config in bigquery_task_configs:
            if task_config.get(consts.QUERY):
                query = task_config.get(consts.QUERY)
            elif task_config.get(consts.QUERY_FILE):
                query_sql_file = f"{settings.DAGS_FOLDER}/{task_config.get(consts.QUERY_FILE)}"
                query = read_file_env(query_sql_file, env)
            else:
                raise AirflowFailException("Please provide query, none found for task.")

            # Get other parameters from task_config
            project_id = task_config.get(consts.PROJECT_ID)
            dataset_id = task_config.get(consts.DATASET_ID)
            source_dataset_id = task_config.get(consts.SOURCE_DATASET_ID)
            source_project_id = task_config.get(consts.SOURCE_PROJECT_ID)
            replacements_dict = task_config.get(consts.REPLACEMENTS, {})
            env_specific_replacements = task_config.get(consts.ENV_SPECIFIC_REPLACEMENTS)
            target_service_account = str(task_config.get(consts.TARGET_SERVICE_ACCOUNT)).replace(consts.ENV_PLACEHOLDER, env) if task_config.get(consts.TARGET_SERVICE_ACCOUNT) else None
            target_project = str(task_config.get(consts.TARGET_PROJECT)).replace(consts.DEPLOY_ENV, env) if task_config.get(consts.TARGET_PROJECT) else None
            execution_timeout = int(config[consts.DAG].get(consts.DAGRUN_TIMEOUT)) if config[consts.DAG].get(consts.DAGRUN_TIMEOUT) else 10080

            table_id = task_config.get(consts.TABLE_ID)
            if table_id:
                table_id = table_id.replace(
                    consts.CURRENT_DATE_PLACEHOLDER,
                    datetime.now(tz=self.local_tz).strftime('%Y_%m_%d')
                )

            task = PythonOperator(
                task_id=task_config.get('task_id'),
                python_callable=BigQueryDbConnector.run_bigquery_staging_query,
                op_kwargs={
                    consts.QUERY: query,
                    consts.PROJECT_ID: project_id,
                    consts.DATASET_ID: dataset_id,
                    consts.TABLE_ID: table_id,
                    consts.SOURCE_PROJECT_ID: source_project_id,
                    consts.SOURCE_DATASET_ID: source_dataset_id,
                    consts.DEPLOY_ENV: env,
                    consts.REPLACEMENTS: replacements_dict,
                    consts.ENV_SPECIFIC_REPLACEMENTS: env_specific_replacements,
                    consts.TARGET_SERVICE_ACCOUNT: target_service_account,
                    consts.TARGET_PROJECT: target_project
                },
                execution_timeout=timedelta(minutes=execution_timeout),
                dag=self.dag
            )
            tasks.append(task)

        tasks.append(end_point)
        return tasks

    def create_dags(self) -> dict:
        """
        Creaxtes a dag for each provided configuration in the bigquery_task_executor_config.yamll file.

        :return: created dags.
        :rtype: dict
        """

        if self.job_config:
            dags = {}
            for job_id, config in self.job_config.items():
                dags[job_id] = self.create_dag(job_id, config)
            return dags
        else:
            return {}
