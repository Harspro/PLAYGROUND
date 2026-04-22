import logging
import os
from airflow import settings
from google.cloud import bigquery, storage
from google.cloud.bigquery.enums import WriteDisposition
from util.constants import ENV_PLACEHOLDER
from alm_processing.utils import bq_util as util
from alm_processing.utils import constants
from util.bq_utils import submit_transformation
from util.gcs_utils import delete_folder, delete_blobs, compose_file, list_blobs_with_prefix, copy_blob
from util.miscutils import read_env_filepattern
logger = logging.getLogger(__name__)


############################################################
# DAG  Methods implementation
############################################################


# TODO:  NEED TO ADD SCHEDULE


def read_sql_file(sql_file_path):
    sql_file = f'{settings.DAGS_FOLDER}/{sql_file_path}'
    if os.path.exists(sql_file):
        with open(sql_file, 'r') as f:
            content = f.read()
            if constants.DEPLOY_ENV is not None and ENV_PLACEHOLDER in content:
                content = content.replace(
                    ENV_PLACEHOLDER, constants.DEPLOY_ENV)
            return content
    else:
        logger.error(f'{sql_file} does not exist')
        raise (Exception(f'{sql_file} does not exist'))


def get_file_name(dag_config: dict, **kwargs):
    dag_date_time = kwargs['data_interval_end']
    dag_date_time = dag_date_time.in_tz(tz=constants.TORONTO_TZ)
    job_date = dag_date_time.strftime('%Y%m%d')
    job_date_time = dag_date_time.strftime('%Y%m%d%H%M%S')
    outbound_file_extension = read_env_filepattern(
        dag_config[constants.FILE][constants.EXTENSION], constants.DEPLOY_ENV)
    filename_format_with_date = f"{constants.DATA_ORIGIN}_{constants.DATA_DEST}_{constants.PROJ_NAME}" \
                                f"_{dag_config[constants.FILE][constants.PREFIX]}_{job_date}" \
                                f".{dag_config[constants.FILE][constants.FORMAT]}_{job_date_time}" \
                                f".{outbound_file_extension}"
    return filename_format_with_date


# ----------------------------------------------------------
# Load data from staging table to main table
# ----------------------------------------------------------
def copy_data_stgtbl_to_maintbl(
        bq_destination_dataset_name,
        bq_feed_table,
        **context):
    sql = f"select * from {constants.PROCESSING_PROJECT_ID}.{bq_destination_dataset_name}.{bq_feed_table}_STG"
    logger.info(
        f'Main table : {constants.CURATED_PROJECT_ID}.{bq_destination_dataset_name}.{bq_feed_table}')
    if context.get("script"):
        job_config = None
    else:
        job_config = bigquery.QueryJobConfig(
            destination=f'{constants.CURATED_PROJECT_ID}.{bq_destination_dataset_name}.{bq_feed_table}',
            write_disposition=WriteDisposition.WRITE_APPEND)
    util.run_bq_query(sql, job_config=job_config)


def copy_data_rawtbl_to_orgtbl(
        bq_destination_dataset_name,
        bq_feed_table,
        **context):
    sql = f"select *,CURRENT_DATETIME() as LOAD_TS from {constants.PROCESSING_PROJECT_ID}.{bq_destination_dataset_name}.{bq_feed_table}_RAW_LATEST"
    logger.info(
        f'Raw table : {constants.LANDING_PROJECT_ID}.{bq_destination_dataset_name}.{bq_feed_table}_HIST')
    if context.get("script"):
        job_config = None
    else:
        job_config = bigquery.QueryJobConfig(
            destination=f'{constants.LANDING_PROJECT_ID}.{bq_destination_dataset_name}.{bq_feed_table}_HIST',
            write_disposition=WriteDisposition.WRITE_APPEND)
    util.run_bq_query(sql, job_config=job_config)


def compose_infinite_files_into_one(
        staging_bucket,
        staging_folder,
        headers_source_file_path,
        headers_destination_file_path,
        bq_to_gcs_folder,
        bq_feed_table,
        **context):
    """
    Takes multiple files and the right column names to combine into one file
    """
    bucket_name = staging_bucket
    header_file_path_source = f"{settings.DAGS_FOLDER}/{headers_source_file_path}"
    header_file_path_destination = f"{headers_destination_file_path}"
    staging_files_location = bq_to_gcs_folder
    logger.info(header_file_path_source)
    logger.info(header_file_path_destination)
    chunk_size: int = 32

    # Get all the source files:
    source_blob_list = list_blobs_with_prefix(
        bucket_name, staging_files_location, delimiter=None)

    # Start the while loop:
    chunk_iteration = 1
    while len(source_blob_list) > chunk_size:
        # Get the chunk
        source_blob_chunk = source_blob_list[:chunk_size]
        logger.info(f"Got the files chunk - {source_blob_chunk}")

        # Compose that chunk
        temp_destination_file = f"{bq_to_gcs_folder}/{bq_feed_table}-composed_chunk_{chunk_iteration}.csv"
        compose_file(bucket_name, source_blob_chunk, temp_destination_file)

        # Delete the chunk files (and not the composed chunk)
        delete_blobs(bucket_name, source_blob_chunk)
        source_blob_list = list_blobs_with_prefix(
            bucket_name, staging_files_location, delimiter=None)
        logger.info(
            f"New files found in the staging folder {staging_files_location} - {source_blob_list}")
        chunk_iteration += 1

    compose_file(bucket_name, source_blob_list, staging_folder)


# -----------------------------------------------------------
# Add Load Date and HDR_TLR_DAT in Staging table
# -----------------------------------------------------------
def update_stage_tbl_metadata(
        bq_destination_dataset,
        bq_table,
        data_name,
        frequency):
    bq_stage_table = f'{bq_table}_STG'
    logger.info(f'stage table : {bq_stage_table}')
    dataset_ref = bigquery.DatasetReference(
        project=f'{constants.PROCESSING_PROJECT_ID}',
        dataset_id=bq_destination_dataset)
    table_ref = bigquery.TableReference(
        dataset_ref=dataset_ref,
        table_id=bq_stage_table)
    client = bigquery.Client()
    table = client.get_table(table_ref)
    org_schema = table.schema
    row_count = table.num_rows
    load_date = constants.CURRENT_DATETIME
    logger.info(f' load_date {load_date}')
    hdr_tlr_str = (
        f"{constants.DATA_ORIGIN}|{constants.DATA_DEST}|{constants.PROJ_NAME}|"
        f"{data_name}|{frequency}|{constants.JOB_DATE}|{row_count}")
    logger.info(f' metadate {hdr_tlr_str}')
    mod_schema = org_schema[:]
    mod_schema.append(bigquery.SchemaField("LOAD_DATE", "DATETIME"))
    mod_schema.append(bigquery.SchemaField("HDR_TLR_DAT", "STRING"))
    logger.info(f' mod_schema:{mod_schema}')
    table.schema = mod_schema
    table = client.update_table(table, ["schema"])
    if len(table.schema) == len(org_schema) + 2 == len(mod_schema):
        logger.info(f'schema update successfully {table.schema}')
    else:
        logger.info(f' failed to update schema {table.schema}')
    sql = (
        f"update {constants.PROCESSING_PROJECT_ID}.{bq_destination_dataset}.{bq_stage_table} "
        f"set LOAD_DATE=@load_date, HDR_TLR_DAT=@hdr_trl_dat WHERE TRUE")
    params = [
        bigquery.ScalarQueryParameter(
            "load_date",
            "DATETIME",
            load_date),
        bigquery.ScalarQueryParameter(
            "hdr_trl_dat",
            "STRING",
            hdr_tlr_str)]
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    util.run_bq_query(sql, job_config=job_config)


# -------------------------------------------------------------------
# Copy data from staging bucket to outbound bucket
# -------------------------------------------------------------------
def copy_and_cleanup(
        staging_bucket,
        staging_folder,
        outbound_bucket,
        outbound_folder,
        config,
        **kwargs):
    # Copy the file and do cleanup
    file_name = get_file_name(config, **kwargs)
    copy_blob(
        staging_bucket,
        f'{staging_folder}/{file_name}',
        outbound_bucket,
        f'{outbound_folder}/{file_name}')
