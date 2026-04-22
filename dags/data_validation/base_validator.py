import logging
from abc import ABC, abstractmethod
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Final

import pendulum
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from data_validation.utils.utils import get_schedule_interval
from util.constants import GCP_CONFIG, DEPLOYMENT_ENVIRONMENT_NAME, DEFAULT_ARGS
from util.miscutils import read_variable_or_file, read_yamlfile_env
from dag_factory.terminus_dag_factory import add_tags

INITIAL_DEFAULT_ARGS: Final = {
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


class BaseValidator(ABC):
    def __init__(
            self,
            config_filename: str,
            config_dir: str = None
    ):
        self.gcp_config = read_variable_or_file(GCP_CONFIG)
        self.deploy_env = self.gcp_config[DEPLOYMENT_ENVIRONMENT_NAME]

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/data_validation/configs'

        self.config_file_path = f'{config_dir}/{config_filename}'
        self.job_config = read_yamlfile_env(self.config_file_path, self.deploy_env)
        self.local_tz = pendulum.timezone('America/Toronto')

    @abstractmethod
    def validate(self, config: dict) -> None:
        """
        Abstract method to be implemented by child classes for specific validation logic.

        :param config: The config for this job in the YAML file.
        """
        pass

    def create_dag(
            self,
            dag_id: str,
            config: dict
    ) -> DAG:
        """
        Creates a DAG with name `dag_id` using details from `config`.

        :param dag_id: The name we will set for the new DAG
        :param config: The config for this job in the YAML file.
        :return: The created DAG
        """
        default_args = deepcopy(INITIAL_DEFAULT_ARGS)
        default_args.update(config.get(DEFAULT_ARGS, INITIAL_DEFAULT_ARGS))
        tags = config.get("tags")
        dag = DAG(
            dag_id=dag_id,
            default_args=default_args,
            schedule=get_schedule_interval(self.deploy_env, config),
            tags=tags,
            start_date=datetime(2024, 1, 1, tzinfo=self.local_tz),
            is_paused_upon_creation=True,
            max_active_runs=1,
            catchup=False,
            dagrun_timeout=timedelta(minutes=15)
        )

        with dag:
            start_point = EmptyOperator(task_id='start')
            end_point = EmptyOperator(task_id='end')

            validate_task = PythonOperator(
                task_id="validate",
                python_callable=self.validate,
                op_kwargs={"config": config}
            )

            start_point >> validate_task >> end_point

        return add_tags(dag)

    def create_dags(
            self
    ) -> dict:
        """
        Creates a DAG for each job listed in the YAML config file.

        :return: A dictionary with DAGs created for all the jobs listed in the YAML config file.
        """
        dags = {}

        if not self.job_config:
            logging.info(f'Config file {self.config_file_path} is empty.')
            return dags

        for job_id, config in self.job_config.items():
            dags[job_id] = self.create_dag(job_id, config)

        return dags
