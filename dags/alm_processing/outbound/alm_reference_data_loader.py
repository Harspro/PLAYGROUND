from alm_processing.validator.alm_file_validator import AlmFileValidator
from etl_framework.etl_dag_base import ETLDagBase
from airflow.exceptions import AirflowFailException
from alm_processing.utils import alm_util as util
from alm_processing.utils import bq_util as bqutil
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from util.bq_utils import submit_transformation, get_table_columns, count_records_tbl
from alm_processing.utils import misc_util as mscutil
from alm_processing.utils import constants
from util.constants import (
    DEFAULT_ARGS, READ_PAUSE_DEPLOY_CONFIG, STAGING_BUCKET,
    STAGING_FOLDER, BIGQUERY, SCHEDULE_INTERVAL, DAG as DAG_STR
)
from util.miscutils import (
    read_yamlfile_env_suffix,
    read_env_filepattern
)
from google.cloud import bigquery
import logging
logger = logging.getLogger(__name__)


class AlmReferenceDataLoader(ETLDagBase):
    def __init__(self, module_name, config_path, config_filename, dag_default_args):
        super().__init__(module_name, config_path, config_filename, dag_default_args)

    def preprocessing_job(self, config: dict, upstream_task: list):
        activity_var = config.get(constants.ACTIVITY_NAME, '')

        task_grp_id = f'{activity_var}_pre_trans_job'
        with TaskGroup(group_id=task_grp_id):
            self.validator = AlmFileValidator(config)
            task = self.validator.validation_task_group(config)
            upstream_task.append(task)
            task = PythonOperator(
                task_id=f"{activity_var}_reference_data_to_bq",
                python_callable=bqutil.load_whole_file,
                op_kwargs={
                    'source_bucket': config[constants.INBOUND_BUCKET],
                    'source_folder': config[constants.INBOUND_FOLDER],
                    'bq_destination_dataset': config[BIGQUERY][constants.BQ_DESTINATION_DATASET],
                    'table_name': config[BIGQUERY][constants.BQ_TABLE_NAME],
                    'filename': config[constants.INBOUND_FILE][constants.PREFIX],
                    'skip_leading_rows': config[constants.INBOUND_FILE][constants.SKIP_LEADING_ROWS]
                },
                trigger_rule=TriggerRule.ALL_SUCCESS)

            upstream_task.append(task)

        return upstream_task

    def postprocessing_job(self, config: dict, upstream_task: list):
        activity_var = config.get(constants.ACTIVITY_NAME, '')

        task_grp_id = f'{activity_var}_post_trans_job'

        with TaskGroup(group_id=task_grp_id):

            task = PythonOperator(
                task_id="validate_ref_table_for_failures",
                python_callable=mscutil.validate_ref_table_for_failures,
                op_kwargs={
                    'bq_destination_dataset': config[BIGQUERY][constants.BQ_DESTINATION_DATASET],
                    'table_name': config[BIGQUERY][constants.BQ_TABLE_NAME]
                },
                trigger_rule=TriggerRule.ALL_SUCCESS)

            upstream_task.append(task)

        return upstream_task
