import copy
from abc import ABC, abstractmethod
from copy import deepcopy
from datetime import timedelta, datetime
from util.constants import (SCHEDULE_INTERVAL, DAG as DAG_STR)
import pendulum
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import logging
import util.constants as constants
import util.bq_utils as bqutil
from etl_framework.utils import misc_util as mscutil
import etl_framework.utils.constants as etl_constants
from util.deploy_utils import (
    read_pause_unpause_setting,
    pause_unpause_dag
)
from util.miscutils import (
    read_yamlfile_env_suffix,
    read_env_filepattern
)
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)


class ETLDagBase(ABC):
    def __init__(self, module_name, config_path, config_filename, dag_default_args):
        self._module_name = f'{module_name}'
        self._config_path = f'{config_path}'
        self.config_dir = f'{settings.DAGS_FOLDER}/{self._module_name}/{self._config_path}'
        config_full_path = f'{self.config_dir}/{config_filename}'
        self.job_config = read_yamlfile_env_suffix(
            f'{config_full_path}',
            etl_constants.DEPLOY_ENV, etl_constants.DEPLOY_ENV_SUFFIX)

        self._default_args = {}

        if dag_default_args:
            self._default_args = deepcopy(dag_default_args)

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
            default_args=self._default_args,
            schedule=self.get_schedule(dag_config),
            start_date=datetime(2024, 1, 1, tzinfo=self.local_tz),
            dagrun_timeout=timedelta(hours=5),
            max_active_runs=1,
            catchup=False,
            is_paused_upon_creation=True,
            tags=self._default_args.get('tags', [])
        )
        with dag:
            is_paused = read_pause_unpause_setting(
                dag_id, etl_constants.DEPLOY_ENV)
            pause_unpause_dag(dag, is_paused)

            dag_config.update({'module_folder': f'{self._module_name}'})

            transformation_tasks = []
            # Add start task
            pipeline_start = EmptyOperator(task_id="start")
            transformation_tasks.append(pipeline_start)

            # Project specific pre_processing execute before transformation
            upstream_task = self.preprocessing_job(dag_config, transformation_tasks)
            if upstream_task:
                transformation_tasks = upstream_task

            # Add transformation task
            if dag_config.get(constants.TRANSFORMATION_CONFIG):
                task_dict = self.create_transformation_tasks(dag_config)
                for key in task_dict.keys():
                    transformation_tasks.append(task_dict[key])

            # Project specific post_trans_config execute after stagging load
            upstream_task = self.postprocessing_job(dag_config, transformation_tasks)
            if upstream_task:
                transformation_tasks = upstream_task

            # Add final task
            final_task = self.run_final_task(dag_config, dag)
            transformation_tasks.append(final_task)

            # Define squential dependencies dynamically
            for i in range(len(transformation_tasks) - 1):
                transformation_tasks[i] >> transformation_tasks[i + 1]

        return add_tags(dag)

    # Define final task
    def run_final_task(self, config: dict, dag: DAG):
        return EmptyOperator(
            task_id="end",
            trigger_rule=TriggerRule.NONE_FAILED)

    @abstractmethod
    def preprocessing_job(self, config: dict):
        pass

    @abstractmethod
    def postprocessing_job(self, config: dict):
        pass

    # Curated transformation task
    def create_transformation_tasks(self, config: dict) -> dict:
        module_name = config.get(constants.MODULE_FOLDER, '')
        transformation_config_path = config.get(constants.TRANSFORMATION_CONFIG, '')
        transformation_config_full_path = f'{settings.DAGS_FOLDER}/{module_name}/{transformation_config_path}'
        logger.info(transformation_config_full_path)

        self.transformation_config = read_yamlfile_env_suffix(
            f'{transformation_config_full_path}',
            etl_constants.DEPLOY_ENV, etl_constants.DEPLOY_ENV_SUFFIX)

        context_parameter = self.transformation_config.get(constants.CONTEXT_PARAMETER, {})

        transformation_tasks = {}

        for task_detail in self.transformation_config[constants.TRANSFORMATION]:
            task_name = task_detail[constants.TASK_NAME]
            if task_detail.get(constants.TASK_PARAM):
                task_config = task_detail[constants.TASK_PARAM]

            # LOAD USING SQL SCRIPT TO BQ
            if task_detail[constants.TASK_TYPE] == etl_constants.SQL_EXTRACT_BQ_TO_BQ:
                script_path = task_config.get(constants.SCRIPT_PATH, '')
                script_full_path = f'{module_name}/{script_path}'
                override_context_parameter = copy.deepcopy(context_parameter)
                if task_config.get(constants.OVERRIDE_PARAMETERS):
                    override_context_parameter.update(task_config.get(constants.OVERRIDE_PARAMETERS))
                task = PythonOperator(
                    task_id=f"{task_name}",
                    python_callable=bqutil.run_bq_script,
                    op_args=[script_full_path,
                             override_context_parameter])
                transformation_tasks[task_name] = task

            # LOAD FILE GCS BUCKET TO BQ
            elif task_detail[constants.TASK_TYPE] == etl_constants.GCS_CSV_TO_BQ:
                task = PythonOperator(
                    task_id=f"{task_name}",
                    python_callable=mscutil.load_file_gcs_bucket_to_bq,
                    op_kwargs={constants.CONFIG: task_config})
                transformation_tasks[task_name] = task

        return transformation_tasks
