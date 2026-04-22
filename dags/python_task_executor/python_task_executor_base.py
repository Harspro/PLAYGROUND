# Standard Library Imports
from importlib import import_module
import logging
import pendulum
import util.constants as consts
from airflow import DAG, settings
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Final, List
from util.miscutils import read_variable_or_file, read_yamlfile_env_suffix

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


class PythonTaskExecutor:
    """Class to generate Airflow DAGs for Executing python callables based on YAML config"""

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

        try:
            self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
            self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
            self.deploy_env_suffix = self.gcp_config[consts.DEPLOY_ENV_STORAGE_SUFFIX]

            if config_dir is None:
                config_dir = f'{settings.DAGS_FOLDER}/config/python_task_executor_configs'

            self.config_dir = config_dir
            self.job_config = read_yamlfile_env_suffix(f'{config_dir}/{config_filename}', self.deploy_env, env_suffix=self.deploy_env_suffix)
        except Exception as error:
            raise AirflowFailException(f'Error while creating config for python task executor. Error: {error}')

    def _create_tasks(self, config: dict) -> List[PythonOperator]:

        """Create Python Callable tasks from config. It expects the python callable
          to have module and method to be imported using the importlib. Please make sure that the modules are valid else
          AirflowFailException is thrown for invalid module.
          We can have sequential number of tasks for one DAG
        """
        tasks = []

        # Empty Operator, used to indicate starting and ending tasks.
        start_point = EmptyOperator(task_id=consts.START_TASK_ID, dag=self.dag)
        end_point = EmptyOperator(task_id=consts.END_TASK_ID, dag=self.dag)

        tasks.append(start_point)

        task_configs = config[consts.TASKS]
        execution_timeout = int(config[consts.DAG].get(consts.DAGRUN_TIMEOUT)) if config[consts.DAG].get(
            consts.DAGRUN_TIMEOUT) else 10080

        for task_config in task_configs:
            if task_config.get(consts.PYTHON_CALLABLE):
                python_callable = task_config.get(consts.PYTHON_CALLABLE)
                module_name = python_callable.get(consts.MODULE)
                method_name = python_callable.get(consts.METHOD)
                try:
                    logger.info(f'method import started : {module_name}.{method_name}')

                    # Importing the methods from modules based on ImportLib
                    module = import_module(module_name)
                    method = getattr(module, method_name)
                    logger.info(f'method imported completed : {module_name}.{method_name}')

                    task = PythonOperator(
                        task_id=task_config.get('task_id'),
                        python_callable=method,
                        op_kwargs=task_config.get(consts.ARGS),
                        execution_timeout=timedelta(minutes=execution_timeout),
                        dag=self.dag
                    )

                    tasks.append(task)
                except Exception:
                    raise AirflowFailException(
                        f"{module_name}.{method_name} is not defined, hence cannot create tasks.")
            else:
                raise AirflowFailException("Please provide python callable, none found for task.")

        tasks.append(end_point)
        return tasks

    def create_dag(self, dag_id: str, config: dict) -> DAG:

        """Create and return an Airflow DAG based on config. This method is called once per DAG config in the
           config yaml file. This will create DAG and list of tasks within that DAG as per configuration
        """

        self.default_args.update(config.get(consts.DEFAULT_ARGS, INITIAL_DEFAULT_ARGS))
        dag_timeout = int(config[consts.DAG].get(consts.DAGRUN_TIMEOUT)) if config[consts.DAG].get(
            consts.DAGRUN_TIMEOUT) else 10080
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

        tasks = self._create_tasks(config=config)

        # Set sequential dependencies
        for i in range(len(tasks) - 1):
            tasks[i] >> tasks[i + 1]

        return self.dag

    def create_dags(self) -> dict:
        """
        Creates a dag for each provided configuration in the python_task_executor_config.yaml file.

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
