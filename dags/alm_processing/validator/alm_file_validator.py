from copy import deepcopy
from datetime import timedelta, datetime
from util.constants import (
    DEFAULT_ARGS, READ_PAUSE_DEPLOY_CONFIG, STAGING_BUCKET,
    STAGING_FOLDER, BIGQUERY, SCHEDULE_INTERVAL, DAG as DAG_STR
)
import pendulum
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from etl_framework.utils.misc_util import get_latest_file
import logging
from alm_processing.utils import alm_validator_util as almvalidatorutils
from alm_processing.utils import bq_util as bqutil
from alm_processing.utils import constants
from util.deploy_utils import (
    read_pause_unpause_setting,
    pause_unpause_dag
)
from util.miscutils import (
    read_yamlfile_env,
    read_env_filepattern
)
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)


class AlmFileValidator:
    def __init__(self, config_filename: str):
        config_dir = f'{settings.DAGS_FOLDER}/alm_processing/dags_config'
        self.job_config = read_yamlfile_env(
            f'{config_dir}/{config_filename}',
            constants.DEPLOY_ENV)
        self.default_args = deepcopy(constants.DAG_DEFAULT_ARGS)
        self.local_tz = pendulum.timezone('America/Toronto')

    def get_schedule(self, dag_config: dict):
        if DAG_STR in dag_config:
            return dag_config[DAG_STR].get(SCHEDULE_INTERVAL, None)
        else:
            return None

    def generate_dags(self) -> dict:
        dags = {}
        for job_id, config in self.job_config.items():
            dags[job_id] = self.create_dag(job_id, config)
        return dags

    def create_dag(self, dag_id: str, dag_config: dict) -> DAG:
        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            schedule=self.get_schedule(dag_config),
            start_date=datetime(2024, 1, 1, tzinfo=self.local_tz),
            dagrun_timeout=timedelta(hours=5),
            max_active_runs=1,
            catchup=False,
            is_paused_upon_creation=True,
            tags=constants.TAGS
        )

        with dag:
            if dag_config.get(READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(
                    dag_id, constants.DEPLOY_ENV)
                pause_unpause_dag(dag, is_paused)

            pipeline_start = EmptyOperator(task_id="start")
            validation_task_group = self.validation_task_group(dag_config)
            end_task = self.run_end_task(dag_config, dag)

            pipeline_start >> validation_task_group >> end_task

        return add_tags(dag)

    def validation_task_group(self, dag_config: dict) -> TaskGroup:
        """Creates a task group with all file validation tasks."""
        with TaskGroup(group_id='file_validation_tasks', prefix_group_id=False) as validation_group:
            validate_file_name_task = self.validate_file_name(dag_config)
            validate_file_size_task = self.validate_file_size(dag_config)
            validate_hdr_trl_task = self.validate_hdr_trl(dag_config)
            validation_completed_task = self.validation_completed(dag_config)

            # Define task dependencies
            validate_file_name_task >> validate_file_size_task >> validate_hdr_trl_task
            validate_file_name_task >> validation_completed_task
            validate_file_size_task >> validation_completed_task
            validate_hdr_trl_task >> validation_completed_task

        return validation_group

    def validate_file_name(self, dag_config: dict):
        return PythonOperator(
            task_id="validate_file_name",
            python_callable=almvalidatorutils.validate_file_name,
            op_args=[dag_config],
            trigger_rule=TriggerRule.NONE_FAILED
        )

    def validate_file_size(self, dag_config: dict):
        return PythonOperator(
            task_id="validate_file_size",
            python_callable=almvalidatorutils.validate_file_size,
            op_args=[dag_config],
            trigger_rule=TriggerRule.ALL_DONE
        )

    def validate_hdr_trl(self, dag_config: dict):
        return PythonOperator(
            task_id="validate_hdr_trl",
            python_callable=almvalidatorutils.validate_hdr_trl,
            op_args=[dag_config],
            trigger_rule=TriggerRule.ALL_DONE
        )

    def validation_completed(self, dag_config: dict):
        """Task to mark validation completion."""
        return EmptyOperator(
            task_id="validation_completed",
            trigger_rule=TriggerRule.NONE_FAILED
        )

    def run_end_task(self, config: dict, dag: DAG):
        return EmptyOperator(
            task_id="end",
            trigger_rule=TriggerRule.NONE_FAILED
        )
