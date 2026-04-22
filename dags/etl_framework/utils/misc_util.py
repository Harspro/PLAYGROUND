import logging
import os
from airflow import settings
from google.cloud import bigquery
from airflow.exceptions import AirflowFailException
from airflow import configuration as conf
from google.cloud import storage
import re
from datetime import datetime
import util.constants as constants
import etl_framework.utils.constants as etl_constants

logger = logging.getLogger(__name__)


############################################################
# Load GCS Bucket to BQ
############################################################

def get_table_ref(config):
    ext_project = config[constants.BIGQUERY][constants.PROJECT_ID]
    ext_dataset = config[constants.BIGQUERY][constants.DATASET_ID]
    ext_table = config[constants.BIGQUERY][constants.TABLE_NAME]
    return f"{ext_project}.{ext_dataset}.{ext_table}"


def load_file_gcs_bucket_to_bq(config: dict):
    logger.info(config)
    source_bucket = config[etl_constants.SOURCE_FILE].get(etl_constants.BUCKET)

    data_folder = config[etl_constants.SOURCE_FILE].get(etl_constants.FOLDER)
    file_pattern = config[etl_constants.SOURCE_FILE].get(etl_constants.FILE_PATTERN)

    logger.info(f"source_bucket:{source_bucket}")
    logger.info(f"source_folder:{data_folder}")
    logger.info(f"file_search_pattern:{file_pattern}")

    filename = get_latest_file(source_bucket, data_folder, file_pattern)
    source_file_path = f"gs://{source_bucket}/{filename}"

    logger.info(f"data_file_path:{filename}")
    if not filename or not source_bucket:
        raise AirflowFailException("Source filename or bucket not specified")

    ext_table_ref = get_table_ref(config)
    logger.info(f"ext_table_ref:{ext_table_ref}")

    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()

    if config[etl_constants.SOURCE_FILE].get(etl_constants.CSV_FILE):
        job_config.source_format = bigquery.SourceFormat.CSV

        if config[etl_constants.SOURCE_FILE][etl_constants.CSV_FILE].get(etl_constants.FIELD_DELIMITER):
            job_config.field_delimiter = config[etl_constants.SOURCE_FILE][etl_constants.CSV_FILE].get(etl_constants.FIELD_DELIMITER)
        else:
            job_config.field_delimiter = "\x07"

        if config[etl_constants.SOURCE_FILE][etl_constants.CSV_FILE].get(etl_constants.SKIP_LEADING_ROWS):
            job_config.skip_leading_rows = config[etl_constants.SOURCE_FILE][etl_constants.CSV_FILE].get(etl_constants.SKIP_LEADING_ROWS)
        else:
            job_config.skip_leading_rows = 0

        if config[etl_constants.SOURCE_FILE][etl_constants.CSV_FILE].get(etl_constants.SCHEMA_EXTERNAL):
            job_config.schema = [bigquery.SchemaField.from_api_repr(s) for s in config[etl_constants.SOURCE_FILE][etl_constants.CSV_FILE].get(etl_constants.SCHEMA_EXTERNAL)]
        else:
            job_config.autodetect = True

    if config[constants.BIGQUERY][constants.DATA_LOAD_TYPE] == constants.FULL_REFRESH:
        client.delete_table(ext_table_ref, not_found_ok=True)
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    else:
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    job_config.ignore_unknown_values = False
    if config[etl_constants.SOURCE_FILE].get(etl_constants.IGNORE_UNKNOWN_VALUES):
        job_config.ignore_unknown_values = config[etl_constants.SOURCE_FILE][etl_constants.IGNORE_UNKNOWN_VALUES]

    job_config.max_bad_records = 0
    if config[etl_constants.SOURCE_FILE].get(etl_constants.MAX_BAD_RECORDS):
        job_config.max_bad_records = config[etl_constants.SOURCE_FILE][etl_constants.MAX_BAD_RECORDS]

    logger.info("Data file loading is statrted")

    load_job = client.load_table_from_uri(
        source_file_path,
        ext_table_ref,
        job_config=job_config)

    load_job.result()
    table_result = client.get_table(ext_table_ref)

    if load_job.done() and load_job.error_result is None:
        logger.info(
            "Table load completed successfully.Loaded {} rows and {} columns to {}"
            .format(table_result.num_rows, len(table_result.schema), ext_table_ref))
    else:
        logger.error(f"Trget_table_refable load failed :{ext_table_ref}")

############################################################
# Get the latest file from GCS bucket using file pattern
############################################################


def get_latest_file(bucket_name, data_folder, file_pattern):
    # Initialize a storage client
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # List all blobs in the bucket
    blobs = bucket.list_blobs(prefix=data_folder)

    # Filter blobs based on file pattern
    matched_blobs = [blob for blob in blobs if re.findall(file_pattern, blob.name)]
    logger.info("matched_blobs", matched_blobs)

    if not matched_blobs:
        logger.error("No files matching the pattern were found.")
        raise (Exception("No files matching the pattern were found"))

    # Find the latest file
    latest_blob = max(matched_blobs, key=lambda b: b.updated)
    logger.info(f"Latest file: {latest_blob.name} (Last updated: {latest_blob.updated})")
    return latest_blob.name
