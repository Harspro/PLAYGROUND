import logging
from google.cloud import bigquery
from util.bq_utils import submit_transformation
from alm_processing.utils import constants
from alm_processing.utils import alm_util as almutils
from google.cloud.bigquery import SchemaField
from util.gcs_utils import delete_folder, delete_blobs, compose_file, list_blobs_with_prefix, copy_blob
from etl_framework.utils.misc_util import get_latest_file
from google.cloud.bigquery.enums import WriteDisposition, DestinationFormat
logger = logging.getLogger(__name__)


# ---------------------------------------------
# Load the latest data in staging table
# ---------------------------------------------
def run_query(
        sql_file_path,
        bq_destination_dataset_name,
        bq_feed_table,
        **context):
    sql = almutils.read_sql_file(sql_file_path)
    bq_feed_stg_table = f'{bq_feed_table}_STG'
    logger.info(
        f'Stagging table : {constants.PROCESSING_PROJECT_ID}.{bq_destination_dataset_name}.{bq_feed_stg_table}')

    if context.get("script"):
        job_config = None
    else:
        job_config = bigquery.QueryJobConfig(
            destination=f'{constants.PROCESSING_PROJECT_ID}.{bq_destination_dataset_name}.{bq_feed_stg_table}',
            write_disposition=WriteDisposition.WRITE_TRUNCATE)
    run_bq_query(sql, job_config=job_config)


# ---------------------------------------------------------------------------
# Read data from staging table and write data into staging bucket file
# ---------------------------------------------------------------------------
def export_bq_table_parquet(
        staging_bucket,
        staging_folder,
        bq_destination_dataset,
        bq_table,
        data_name,
        frequency,
        config,
        **kwargs):
    bq_stage_table = f'{bq_table}_STG'
    bucket_name = staging_bucket
    file_name = almutils.get_file_name(config, **kwargs)
    staging_file = f'{staging_folder}/{file_name}'
    destination_folder = f"gs://{bucket_name}/{staging_file}"
    logger.info(f"staging_file : {staging_file}")
    logger.info(f"destination_folder : {destination_folder}")
    logger.info(f"export table: : {bq_destination_dataset}.{bq_stage_table}")

    # Remove any previous files:
    delete_folder(bucket_name, staging_file)
    stage_tmp_tbl = f'{bq_table}_TMP'
    source_view = f'{constants.PROCESSING_PROJECT_ID}.{bq_destination_dataset}.{bq_stage_table}'
    destination_view = f'{constants.PROCESSING_PROJECT_ID}.{bq_destination_dataset}.{stage_tmp_tbl}'
    client = bigquery.Client()
    source_schema = client.get_table(source_view).schema
    column_string = ",".join(
        [f'CAST({field.name} AS STRING) AS {field.name}' for field in source_schema if field.name != 'LOAD_DATE'])
    # do not export load date to parquet
    temp_stage_ddl = f"""
                        CREATE OR REPLACE TABLE {destination_view}
                        OPTIONS (
                            expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(),
                            INTERVAL 3 HOUR),
                            description = "alm temp table for storing extract without load date "
                        )
                        AS (SELECT  {column_string} FROM {source_view})
                        """

    submit_transformation(
        client,
        f'{constants.PROCESSING_PROJECT_ID}.{bq_destination_dataset}.{stage_tmp_tbl}',
        temp_stage_ddl)
    # Export the files to gcs location
    # no headers - in parquest format
    job_config = bigquery.ExtractJobConfig(
        destination_format=DestinationFormat.PARQUET,
        compression=bigquery.Compression.SNAPPY)
    export_bq_to_gcs(
        constants.PROCESSING_PROJECT_ID,
        bq_destination_dataset,
        stage_tmp_tbl,
        destination_folder,
        job_config)


def load_whole_file(
        source_bucket,
        source_folder,
        bq_destination_dataset,
        table_name,
        filename, skip_leading_rows, **context):
    latest_filename = context['ti'].xcom_pull(
        task_ids='validate_file_name', key='latest_filename')
    file_uri = f"gs://{source_bucket}/{latest_filename}"
    schema = [{"name": "ALL_RECORDS", "type": "STRING", "mode": "NULLABLE"}]
    # bq_table_id = f'{constants.CURATED_PROJECT_ID}.{bq_destination_dataset}.{table_name}_RAW_TMP'
    bq_table_id = f'{constants.PROCESSING_PROJECT_ID}.{bq_destination_dataset}.{table_name}_RAW_TMP'
    load_csv_gcs_to_bq(
        file_uri,
        bq_table_id,
        schema,
        None,
        skip_leading_rows,
        quote_char='')


def load_csv_gcs_to_bq(
        file_uri,
        bq_table_id,
        schema=None,
        delimiter=None,
        skip_leading_rows=None,
        load_type=None,
        ignore_unknown_values=None,
        max_bad_records=None,
        quote_char=None):
    bq_client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    if delimiter:
        job_config.field_delimiter = delimiter
        logger.info(f'delmiter is {delimiter}')
    else:
        job_config.field_delimiter = '*'
    if skip_leading_rows:
        job_config.skip_leading_rows = skip_leading_rows
    else:
        job_config.skip_leading_rows = 0
    if schema:
        job_config.schema = [
            bigquery.SchemaField.from_api_repr(s) for s in schema]
    else:
        job_config.autodetect = True
    if load_type == "APPEND":
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    else:
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    if ignore_unknown_values:
        job_config.ignore_unknown_values = ignore_unknown_values
    else:
        job_config.ignore_unknown_values = False
    if max_bad_records:
        job_config.max_bad_records = max_bad_records
    else:
        job_config.max_bad_records = 0
    if quote_char is not None:
        job_config.quote_character = quote_char
    else:
        job_config.quote_character = '"'
    bq_load_job = bq_client.load_table_from_uri(
        file_uri, f"{bq_table_id}", job_config=job_config
    )
    bq_load_job.result()
    table_result = bq_client.get_table(bq_table_id)
    if bq_load_job.done() and bq_load_job.error_result is None:
        logger.info(
            "Table load completed successfully.Loaded {} rows and {} columns to {}" .format(
                table_result.num_rows, len(
                    table_result.schema), bq_table_id))
    else:
        logger.error(f"Trget_table_refable load failed :{bq_table_id}")


def export_bq_to_gcs(
        project_id,
        bq_destination_dataset,
        table_name,
        dest_file_uri,
        job_config):
    client = bigquery.Client()
    dataset_ref = bigquery.DatasetReference(
        project=project_id,
        dataset_id=bq_destination_dataset)
    table_ref = bigquery.TableReference(
        dataset_ref=dataset_ref, table_id=table_name)
    extract_job = client.extract_table(
        source=table_ref,
        destination_uris=dest_file_uri,
        job_config=job_config)
    extract_job.result()
    if extract_job.done() and extract_job.error_result is None:
        logger.info("Extract job completed successfully")


def run_bq_query(sql_query, job_config=None, timeout_seconds=60 * 30):
    sql = sql_query
    nl = "\n"
    logger.info(f'Query to be executed : {nl}{sql}')
    client = bigquery.Client()
    query_job = client.query(sql, job_config=job_config)
    query_job.result(timeout=timeout_seconds, retry=None)
    if query_job.done() and query_job.error_result is None:
        logger.info("Query job completed successfully")
    return query_job


def run_bq_dml_with_log(
        pre: str,
        post: str,
        sql: str,
        job_config: bigquery.QueryJobConfig = None):
    logger.info(f"{pre},SQL:${sql}")
    query_job = run_bq_query(sql, job_config)
    logger.info("Affected rows: " + str(query_job.num_dml_affected_rows))
    logger.info(f"{post}")


def run_bq_select(
        sql_select: str,
        job_config: bigquery.QueryJobConfig = None) -> bigquery.table.RowIterator:
    sql = sql_select
    client = bigquery.Client()
    logger.info(f"Running select SQL:\n ${sql}")
    query_job = client.query(sql, job_config=job_config)
    results = query_job.result()
    if query_job.done() and query_job.error_result is None:
        logger.info("Query job completed successfully")
    else:
        logger.error(f"BQ select failed:${query_job.errors}\nsql:${sql}")
    return results
