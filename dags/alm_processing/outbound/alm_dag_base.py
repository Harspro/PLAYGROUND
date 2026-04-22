from abc import ABC, abstractmethod
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
from airflow.utils.trigger_rule import TriggerRule
import logging
from alm_processing.utils import alm_util as almutils
from alm_processing.utils import bq_util as bqutil
from alm_processing.utils import constants
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


class AlmDagBase(ABC):
    def __init__(self, config_filename: str):
        config_dir = f'{settings.DAGS_FOLDER}/alm_processing/dags_config'
        self.job_config = read_yamlfile_env_suffix(
            f'{config_dir}/{config_filename}',
            constants.DEPLOY_ENV, constants.DEPLOY_ENV_SUFFIX)
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
            render_template_as_native_obj=True,
            tags=constants.TAGS
        )

        with dag:
            if dag_config.get(READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(
                    dag_id, constants.DEPLOY_ENV)
                pause_unpause_dag(dag, is_paused)

            pipeline_start = EmptyOperator(task_id="start")
            get_data_task = self.get_data(dag_config)
            update_stage_tbl_metadata_task = self.update_stage_tbl_metadata(
                dag_config)
            copy_data_stgtbl_to_maintbl_task = self.copy_data_stgtbl_to_maintbl(
                dag_config)
            export_table_to_gcs_parquet_task = self.export_table_to_gcs_parquet(
                dag_config)
            copy_object_to_outbound_bucket_task = self.copy_object_to_outbound_bucket(
                dag_config)
            final_task = self.run_final_task(dag_config, dag)

            pipeline_start >> get_data_task >> update_stage_tbl_metadata_task >> copy_data_stgtbl_to_maintbl_task >> \
                export_table_to_gcs_parquet_task >> copy_object_to_outbound_bucket_task >> final_task

        return add_tags(dag)

    @abstractmethod
    def preprocessing_job(self, dag_config: dict):
        pass

    @abstractmethod
    def postprocessing_job(self, dag_config: dict):
        pass

    @abstractmethod
    def create_dataproc_job(self, dag_config: dict):
        pass

    @abstractmethod
    def process_file(self, dag_config: dict):
        pass

    @abstractmethod
    def get_data(self, config: dict):
        pass

    def update_stage_tbl_metadata(self, config: dict):
        return PythonOperator(
            task_id="update_stage_tbl_hdr_trl_metadata",
            python_callable=almutils.update_stage_tbl_metadata,
            op_args=[config[BIGQUERY][constants.BQ_DESTINATION_DATASET],
                     config[BIGQUERY][constants.BQ_TABLE_NAME], config[constants.FILE][constants.PREFIX],
                     config[constants.FILE][constants.FREQUENCY]],
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

    def copy_data_stgtbl_to_maintbl(self, config: dict):
        return PythonOperator(
            task_id="load_data_from_stgtbl_to_maintbl",
            python_callable=almutils.copy_data_stgtbl_to_maintbl,
            op_args=[config[BIGQUERY][constants.BQ_DESTINATION_DATASET],
                     config[BIGQUERY][constants.BQ_TABLE_NAME]],
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

    def export_table_to_gcs_parquet(self, config: dict):
        return PythonOperator(task_id="export_table_to_gcs",
                              python_callable=bqutil.export_bq_table_parquet,
                              op_args=[config[STAGING_BUCKET],
                                       config[STAGING_FOLDER],
                                       config[BIGQUERY][constants.BQ_DESTINATION_DATASET],
                                       config[BIGQUERY][constants.BQ_TABLE_NAME],
                                       config[constants.FILE][constants.PREFIX],
                                       config[constants.FILE][constants.FREQUENCY], config],
                              trigger_rule=TriggerRule.ALL_SUCCESS)

    def copy_object_to_outbound_bucket(self, config: dict):
        return PythonOperator(
            task_id="copy_object_to_outbound_bucket_task",
            python_callable=almutils.copy_and_cleanup,
            op_args=[config[STAGING_BUCKET], config[STAGING_FOLDER],
                     config[constants.OUTBOUND_BUCKET], config[constants.OUTBOUND_FOLDER], config],
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

    def run_final_task(self, config: dict, dag: DAG):
        return EmptyOperator(
            task_id="end",
            trigger_rule=TriggerRule.NONE_FAILED)

# rundown of basic dag steps
# 1. get_data -> will read data from source and save to table
# 2. update_stage_tbl_metadata -> update table  with header and trailer metadata
# 3. export table -> use api to export table data into a file into extract folder
# 4. copy final file to outbound landing bucket and rename appropriately
