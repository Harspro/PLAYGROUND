from alm_processing.outbound.alm_vendor_data_loader import AlmVendorDataLoader
from copy import deepcopy
from alm_processing.utils import alm_util as util
from alm_processing.utils import bq_util as bqutil
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from alm_processing.utils import constants
from alm_processing.validator.alm_file_validator import AlmFileValidator
from alm_processing.utils import alm_validator_util as almvalidatorutils
from alm_processing.utils import alm_util as almutils
from airflow.utils.task_group import TaskGroup
from google.cloud.bigquery.enums import DestinationFormat
from util.bq_utils import submit_transformation
from util.constants import (
    DEFAULT_ARGS, READ_PAUSE_DEPLOY_CONFIG, STAGING_BUCKET,
    STAGING_FOLDER, BIGQUERY, SCHEDULE_INTERVAL, DAG as DAG_STR
)
from google.cloud import bigquery
import logging
logger = logging.getLogger(__name__)


class AlmSharedDataLoader(AlmVendorDataLoader):
    def __init__(self, config_filename: str):
        super().__init__(config_filename)
        self.updated_config = None

    def initialize_updated_config(self, config: dict):
        """
        Method to replace vendor placeholders and initialize updated_config.
        """
        self.updated_config = self.replace_vendor_placeholders(config)

    #   NOTE:  This function has to be implemented as source of data
    def get_data(self, config: dict):
        self.initialize_updated_config(config)
        with TaskGroup(group_id='get_data', prefix_group_id=False) as shared_file_processing_group:
            vendor_file_pre_process_task = self.preprocessing_job(config)
            vendor_file_filtering_task = self.filter_file(self.updated_config)
            mapping_task = self.map_data(self.updated_config)

            vendor_file_pre_process_task >> vendor_file_filtering_task >> mapping_task
        return shared_file_processing_group

    #   TODO: PLACEHOLDER FOR  STEPS NEEDED FOR PREPROCESSING
    def preprocessing_job(self, dag_config: dict):
        with TaskGroup(group_id='preprocessing_job', prefix_group_id=False) as preprocess_group:
            # Task 1: Validate vendor
            validate_vendor_task = self.validate_vendor(dag_config)
            self.validator = AlmFileValidator(self.updated_config)
            # Task 2: Validate file
            validate_file_task = self.validator.validation_task_group(self.updated_config)
            # Task 3: Load vendor file to BigQuery
            load_vendor_file_to_bq = PythonOperator(
                task_id="load_vendor_file_to_bq",
                python_callable=bqutil.load_whole_file,
                op_args=[self.updated_config[constants.INBOUND_BUCKET],
                         self.updated_config[constants.INBOUND_FOLDER],
                         self.updated_config[BIGQUERY][constants.BQ_DESTINATION_DATASET],
                         self.updated_config[BIGQUERY][constants.BQ_TABLE_NAME],
                         self.updated_config[constants.INBOUND_FILE][constants.PREFIX],
                         self.updated_config[constants.INBOUND_FILE].get(constants.SKIP_LEADING_ROWS, 0)],
                trigger_rule=TriggerRule.ALL_SUCCESS
            )

            # Set dependencies inside the TaskGroup
            validate_vendor_task >> validate_file_task >> load_vendor_file_to_bq

        return preprocess_group

    #   TODO: PLACEHOLDER FOR  STEPS NEEDED FOR POSTPROCESSING
    def postprocessing_job(self, dag_config: dict):
        pass

    #   TODO:  NEED TO ADD DATAPROC FUNCTIONALITY WHEN NEEDED
    def create_dataproc_job(self, dag_config: dict):
        pass

    #   TODO:  NEED TO ADD PARSE FILE FUNCTIONALITY FOR EXTERNAL VENDOR FILES
    def process_file(self, dag_config: dict):
        pass

    def validate_vendor(self, dag_config: dict):
        return PythonOperator(
            task_id="validate_vendor",
            python_callable=almvalidatorutils.validate_vendor,
            op_args=[dag_config],
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

    def update_stage_tbl_metadata(self, config: dict):
        return PythonOperator(
            task_id="update_stage_tbl_hdr_trl_metadata",
            python_callable=almutils.update_stage_tbl_metadata,
            op_args=[self.updated_config[BIGQUERY][constants.BQ_DESTINATION_DATASET],
                     self.updated_config[BIGQUERY][constants.BQ_TABLE_NAME], self.updated_config[constants.FILE][constants.PREFIX],
                     self.updated_config[constants.FILE][constants.FREQUENCY]],
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

    def copy_data_stgtbl_to_maintbl(self, config: dict):
        return PythonOperator(
            task_id="load_data_from_stgtbl_to_maintbl",
            python_callable=almutils.copy_data_stgtbl_to_maintbl,
            op_args=[self.updated_config[BIGQUERY][constants.BQ_DESTINATION_DATASET],
                     self.updated_config[BIGQUERY][constants.BQ_TABLE_NAME]],
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

    def export_table_to_gcs_parquet(self, config: dict):
        return PythonOperator(task_id="export_table_to_gcs",
                              python_callable=bqutil.export_bq_table_parquet,
                              op_args=[self.updated_config[STAGING_BUCKET],
                                       self.updated_config[STAGING_FOLDER],
                                       self.updated_config[BIGQUERY][constants.BQ_DESTINATION_DATASET],
                                       self.updated_config[BIGQUERY][constants.BQ_TABLE_NAME],
                                       self.updated_config[constants.FILE][constants.PREFIX],
                                       self.updated_config[constants.FILE][constants.FREQUENCY], self.updated_config],
                              trigger_rule=TriggerRule.ALL_SUCCESS)

    def copy_object_to_outbound_bucket(self, config: dict):
        return PythonOperator(
            task_id="copy_object_to_outbound_bucket_task",
            python_callable=almutils.copy_and_cleanup,
            op_args=[self.updated_config[STAGING_BUCKET], self.updated_config[STAGING_FOLDER],
                     self.updated_config[constants.OUTBOUND_BUCKET], self.updated_config[constants.OUTBOUND_FOLDER], self.updated_config],
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

    def replace_vendor_placeholders(self, config):
        """Helper function to get vendor names from XCom and replace placeholders dynamically."""
        config_copy = deepcopy(config)

        vendor_name_lower = "{{ ti.xcom_pull(task_ids='validate_vendor', key='vendor_name_lower') }}"
        vendor_name_upper = "{{ ti.xcom_pull(task_ids='validate_vendor', key='vendor_name_upper') }}"

        lower_placeholder = f"{{{config.get(constants.LOWERCASE_VENDOR_NAME, 'vendor_name')}}}"
        upper_placeholder = f"{{{config.get(constants.UPPERCASE_VENDOR_NAME, 'VENDOR_NAME')}}}"

        def replace_placeholder(value, lower_placeholder, upper_placeholder):
            if isinstance(value, str):
                value = value.replace(lower_placeholder, vendor_name_lower)
                value = value.replace(upper_placeholder, vendor_name_upper)
            return value

        for key, value in config_copy.items():
            if isinstance(value, dict):
                config_copy[key] = self.replace_vendor_placeholders(value)
            else:
                config_copy[key] = replace_placeholder(value, lower_placeholder, upper_placeholder)

        return config_copy
