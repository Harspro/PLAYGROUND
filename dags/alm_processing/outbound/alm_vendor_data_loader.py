from alm_processing.outbound.alm_dag_base import AlmDagBase
from alm_processing.utils import alm_util as util
from alm_processing.utils import bq_util as bqutil
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from alm_processing.utils import constants
from util.gcs_utils import read_file_bytes, read_file, write_file
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


class AlmVendorDataLoader(AlmDagBase):
    def __init__(self, config_filename: str):
        super().__init__(config_filename)

    #   NOTE:  This function has to be implemented as source of data
    def get_data(self, config: dict):
        with TaskGroup(group_id='get_data', prefix_group_id=False) as preprocessing_group:
            vendor_file_pre_process_task = self.preprocessing_job(config)
            vendor_file_filtering_task = self.filter_file(config)
            mapping_task = self.map_data(config)

            vendor_file_pre_process_task >> vendor_file_filtering_task >> mapping_task
        return preprocessing_group

    #   TODO: PLACEHOLDER FOR  STEPS NEEDED FOR PREPROCESSING
    def preprocessing_job(self, dag_config: dict):
        with TaskGroup(group_id='preprocessing_job', prefix_group_id=False) as preprocess_group:
            self.validator = AlmFileValidator(dag_config)
            # Task 1: Validate file
            validate_file_task = self.validator.validation_task_group(
                dag_config)
            # Task 2: Load vendor file to BigQuery
            load_vendor_file_to_bq = PythonOperator(
                task_id="load_vendor_file_to_bq",
                python_callable=bqutil.load_whole_file,
                op_args=[dag_config[constants.INBOUND_BUCKET],
                         dag_config[constants.INBOUND_FOLDER],
                         dag_config[BIGQUERY][constants.BQ_DESTINATION_DATASET],
                         dag_config[BIGQUERY][constants.BQ_TABLE_NAME],
                         dag_config[constants.INBOUND_FILE][constants.PREFIX],
                         dag_config[constants.INBOUND_FILE][constants.SKIP_LEADING_ROWS]],
                trigger_rule=TriggerRule.ALL_SUCCESS
            )

            # Set dependencies inside the TaskGroup
            validate_file_task >> load_vendor_file_to_bq

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

    def filter_file(self, dag_config: dict):
        return PythonOperator(
            task_id="filter_vendor_data",
            python_callable=self.filter_vendor_data,
            op_args=[dag_config[constants.INBOUND_BUCKET],
                     dag_config[constants.INBOUND_FOLDER],
                     dag_config[BIGQUERY][constants.BQ_DESTINATION_DATASET],
                     dag_config[BIGQUERY][constants.BQ_TABLE_NAME],
                     dag_config[constants.INBOUND_FILE][constants.PREFIX],
                     dag_config[STAGING_BUCKET],
                     dag_config[STAGING_FOLDER],
                     dag_config[constants.INBOUND_FILE][constants.SCHEMA_EXTERNAL],
                     dag_config[constants.INBOUND_FILE][constants.DELIMITER]],
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

    def filter_vendor_data(self,
                           source_bucket,
                           source_folder,
                           bq_destination_dataset,
                           table_name,
                           filename,
                           staging_bucket,
                           staging_folder,
                           schema,
                           delimiter):
        # bq_table_id = f'{constants.CURATED_PROJECT_ID}.{bq_destination_dataset}.{table_name}'
        bq_table_id = f'{constants.PROCESSING_PROJECT_ID}.{bq_destination_dataset}.{table_name}'
        source_tmp_tbl = f'{bq_table_id}_RAW_TMP'
        dest_tmp_tbl = F'{bq_table_id}_DATA_TMP'
        client = bigquery.Client()
        # do not export load date to parquet
        temp_stage_ddl = f"""
                               CREATE OR REPLACE TABLE {dest_tmp_tbl}
                               OPTIONS (
                                   expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(),
                                   INTERVAL 3 HOUR),
                                   description = "alm temp table for storing extract without load date "
                               )
                               AS (SELECT  * FROM {source_tmp_tbl} WHERE LOWER(ALL_RECORDS) NOT like '%header%' AND
                                    LOWER(ALL_RECORDS) NOT like  '%footer%' AND LOWER(ALL_RECORDS) NOT like  '%trailer%'
                                    AND LOWER(ALL_RECORDS) NOT like  'end-of-data%'
                                    AND LOWER(ALL_RECORDS) NOT like  'timefinished%'
                                    AND LOWER(ALL_RECORDS) NOT like  'end-of-file%'
                                    )
                               """

        submit_transformation(
            client,
            dest_tmp_tbl,
            temp_stage_ddl)
        dest_file_uri = f"gs://{staging_bucket}/{staging_folder}/{filename}_DAT"
        export_temp_stage_ddl = f""" select * from {dest_tmp_tbl} """
        """Use pandas to export file to gcs , as bigquery extract functionality
                is considering data as a single row and quoting it  """
        temp_df = client.query(export_temp_stage_ddl).to_dataframe()
        write_file(
            staging_bucket,
            f'{staging_folder}/{filename}_DAT',
            temp_df.to_string(
                header=False,
                index=False).strip())
        # job_config = bigquery.ExtractJobConfig(
        #     destination_format=DestinationFormat.CSV,
        #     print_header=False,
        #     field_delimiter=delimiter)
        # bqutil.export_bq_to_gcs(
        #     f'{constants.PROCESSING_PROJECT_ID}',
        #     bq_destination_dataset,
        #     f'{table_name}_DATA_TMP',
        #     dest_file_uri, job_config)

        logger.info(f'delmiter is {delimiter}')
        bqutil.load_csv_gcs_to_bq(
            dest_file_uri,
            f'{bq_table_id}_RAW_LATEST',
            schema,
            delimiter)

    def map_vendor_data(
            self,
            bq_destination_dataset,
            table_name,
            file_config):
        mapping_config = file_config.get(constants.DATA_MAPPING, None)
        sql_path = mapping_config.get(
            constants.MAPPING_SQL,
            None) if mapping_config else None
        col_map = mapping_config.get(
            constants.COLUMN_MAP,
            None) if mapping_config else None
        data_filter = mapping_config.get(
            constants.DATA_FILTER,
            None) if mapping_config else None
        bq_table_id = f'{constants.PROCESSING_PROJECT_ID}.{bq_destination_dataset}.{table_name}'
        almutils.copy_data_rawtbl_to_orgtbl(bq_destination_dataset, table_name)
        client = bigquery.Client()
        if sql_path:
            sql = almutils.read_sql_file(sql_path)
        elif col_map:
            select_cols = " , ".join(
                f'{v} AS {k}' for (
                    k, v) in col_map.items())
            if data_filter:
                sql = f' select {select_cols} from {bq_table_id}_RAW_LATEST where {data_filter} '
            else:
                sql = f' select {select_cols} from {bq_table_id}_RAW_LATEST '

        else:
            sql = f' select * from {bq_table_id}_RAW_LATEST'

        stage_ddl = f""" CREATE OR REPLACE TABLE {bq_table_id}_STG
                           OPTIONS (
                               expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(),
                               INTERVAL 3 HOUR),
                               description = "alm temp table for storing RAW extract without load date "
                           )
                            AS ({sql}) """
        submit_transformation(
            client,
            f'{bq_table_id}_STG',
            stage_ddl)

    def map_data(self, dag_config):
        return PythonOperator(
            task_id="map_vendor_data",
            python_callable=self.map_vendor_data,
            op_args=[
                dag_config[BIGQUERY][constants.BQ_DESTINATION_DATASET],
                dag_config[BIGQUERY][constants.BQ_TABLE_NAME],
                dag_config[constants.INBOUND_FILE]],
            trigger_rule=TriggerRule.ALL_SUCCESS
        )
