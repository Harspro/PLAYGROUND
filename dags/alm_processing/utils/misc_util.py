import logging
import os
from airflow import settings
from google.cloud import bigquery
from alm_processing.utils import constants
from util.constants import BIGQUERY, SCRIPT_PATH, CONTEXT_PARAMETER
from util.bq_utils import check_bq_table_exists
from util.bq_utils import submit_transformation
from util.bq_utils import run_bq_script
from util.bq_utils import count_records_tbl
from airflow.exceptions import AirflowFailException

logger = logging.getLogger(__name__)

############################################################
# DAG  Methods implementation
############################################################


############################################################
# Create Ref Tables Schema
############################################################
def create_schema(schema_file_path):
    if os.path.exists(schema_file_path):
        with open(schema_file_path, 'r') as f:
            contents = f.readlines()
            schema = []
            for lines in contents:
                split_rec = lines.strip('\n').split(',')
                schema.append(bigquery.SchemaField(split_rec[0], split_rec[1]))
        return schema
    else:
        logger.error(f"{schema_file_path} does not exist")
        raise (Exception(f"{schema_file_path} does not exist"))


############################################################
# Load Ref Tables
############################################################
def load_data_to_ref_tables(
        prefix,
        ref_tbl_fld,
        bq_destination_project,
        bq_destination_dataset,
        bq_table_name,
        **context):
    file_data_path = f'{settings.DAGS_FOLDER}/{ref_tbl_fld}/{prefix}.csv'
    file_schema_path = f'{settings.DAGS_FOLDER}/{ref_tbl_fld}/{prefix}.schema'

    logger.info(f"Data File Path: {file_data_path}")
    logger.info(f"Schema Path: {file_schema_path}")

    client = bigquery.Client()
    table_ref = f"{bq_destination_project}.{bq_destination_dataset}.{bq_table_name}"

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.schema = create_schema(file_schema_path)
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1

    logger.info(f"job_config: {job_config}")

    with open(file_data_path, "rb") as source_file:
        load_job = client.load_table_from_file(
            source_file,
            table_ref,
            job_config=job_config)

    load_job.result()
    table_result = client.get_table(table_ref)

    if load_job.done() and load_job.error_result is None:
        logger.info("Table load completed successfully.Loaded {} rows and {} columns to {}"
                    .format(table_result.num_rows, len(table_result.schema), table_ref))
    else:
        logger.error(f"Table load failed :{table_ref}")


def validate_ref_table_for_failures(bq_destination_dataset, table_name):
    """
    Validates if the invalid record tables contain failures.
    - The DAG will fail if the invalid table contains records.
    """
    bq_invalid_table_id = f'{constants.PROCESSING_PROJECT_ID}.{bq_destination_dataset}.{table_name}_INVALID_TMP'
    logger.info(f"bq_invalid_table_id: {bq_invalid_table_id}")
    invalid_count = count_records_tbl(bq_invalid_table_id)
    logger.info(f"invalid_count: {invalid_count}")
    if invalid_count:
        raise AirflowFailException(f"Process failed! invalid records found in {bq_invalid_table_id}. Check logs for details.")

    logging.info("No invalid records found. DAG will proceed successfully.")
