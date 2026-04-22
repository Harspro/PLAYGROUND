import logging
import time
import csv
import io
from dataclasses import is_dataclass, fields, dataclass
from datetime import datetime, date, time as datetime_time
from typing import Union, Mapping, List, Optional, Tuple, Dict, Any

import numpy as np
import pandas as pd
import google.auth
import google.auth.transport.requests
import os

try:
    from airflow import settings as airflow_settings
    from airflow.exceptions import AirflowException
    from airflow.models import Variable
except Exception:
    airflow_settings = None

    class AirflowException(Exception):
        pass

    Variable = None
from google.auth import impersonated_credentials

try:
    from google.cloud import bigquery
    from google.cloud.bigquery import SqlTypeNames, WriteDisposition, QueryJobConfig, QueryJob
    from google.cloud.bigquery.table import Table
    from google.cloud.exceptions import NotFound
except Exception:
    bigquery = None

    class SqlTypeNames:  # type: ignore
        STRING = "STRING"
        BYTES = "BYTES"
        INTEGER = "INTEGER"
        INT64 = "INT64"
        FLOAT = "FLOAT"
        FLOAT64 = "FLOAT64"
        BOOLEAN = "BOOLEAN"
        BOOL = "BOOL"
        STRUCT = "STRUCT"
        DATE = "DATE"
        TIME = "TIME"
        DATETIME = "DATETIME"
        TIMESTAMP = "TIMESTAMP"

    class WriteDisposition:  # type: ignore
        WRITE_APPEND = "WRITE_APPEND"
        WRITE_TRUNCATE = "WRITE_TRUNCATE"
        WRITE_EMPTY = "WRITE_EMPTY"

    class QueryJobConfig:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise RuntimeError("google-cloud-bigquery is required for QueryJobConfig.")

    class QueryJob:  # type: ignore
        pass

    class Table:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise RuntimeError("google-cloud-bigquery is required for Table.")

    class NotFound(Exception):
        pass

try:
    from google.cloud import storage
except Exception:
    storage = None

try:
    import requests
except ImportError:
    requests = None

import util.constants as consts
from util.miscutils import read_variable_or_file

logger = logging.getLogger(__name__)

DAGS_FOLDER = (
    airflow_settings.DAGS_FOLDER
    if airflow_settings is not None
    else os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
)


def load_record_to_bq(table_id: str,
                      o: dataclass,
                      load_method: WriteDisposition = WriteDisposition.WRITE_APPEND) -> None:
    """
    This method loads python object `o` as a record in the BQ table identified by `table_id`,
    using write disposition `load_method`.

    The method does not assume that the table already exists.

    :param table_id: The unique identifier of the table.
    :param o: The python object to be loaded to BQ table.
    :param load_method: The action that occurs if destination table already exists.
    """
    load_records_to_bq(table_id=table_id, records=[o], load_method=load_method)


def load_records_to_bq(table_id: str,
                       records: List[dataclass],
                       load_method: WriteDisposition = WriteDisposition.WRITE_TRUNCATE,
                       timestamp=False) -> None:
    """
    This method loads the sequence of python objects `lst` as records in the BQ table identified
    by `table_id`, using write disposition `load_method`.

    The method does not assume that the table already exists.

    Precondition: All python objects in the `lst` have the same type and are dataclasses.

    :param table_id: The unique identifier of the table
    :param records: The list of python objects to be loaded to BQ table.
    :param load_method: The action that occurs if destination table already exists
    :param timestamp: If true, set BQ column type for datetime object to TIMESTAMP,DATETIME otherwise.
    """
    if bigquery is None:
        raise RuntimeError(
            "google-cloud-bigquery is required to load records. "
            "Install it in your venv before running this script."
        )
    schema = get_bq_schema(records[0], timestamp=timestamp)
    logging.info(f"Transforming {len(records)} record(s) from of type {records[0].__class__.__name__} to dictionary.")
    lst = [o.__dict__ for o in records]
    logging.info(f"{len(records)} record(s) transformed to dictionary.")
    logging.info(f"Transforming {len(records)} record(s) from of type dict to dataframe.")
    df = pd.DataFrame.from_records(lst)
    logging.info(f"{len(records)} record(s) transformed to dataframe.")
    for col, col_type in schema.items():
        if hasattr(df.get(col).dtype, "char") and df.get(col).dtype.char == 'O':
            logging.info(f"Transforming column {col} from {df[col].dtype}.")

            if col_type == SqlTypeNames.STRING:
                df[col] = df[col].astype(np.str_)
            elif col_type == SqlTypeNames.BYTES:
                df[col] = df[col].astype(np.core.bytes_)
            elif col_type == SqlTypeNames.INTEGER or col_type == SqlTypeNames.INT64:
                df[col] = df[col].astype(np.int64)
            elif col_type == SqlTypeNames.FLOAT or col_type == SqlTypeNames.FLOAT64:
                df[col] = df[col].astype(np.float64)
            elif col_type == SqlTypeNames.BOOLEAN or col_type == SqlTypeNames.BOOL:
                df[col] = df[col].astype(np.bool8)
            elif col_type == SqlTypeNames.TIME or col_type == SqlTypeNames.DATE or col_type == SqlTypeNames.DATETIME:
                df[col] = pd.to_datetime(df[col], utc=True).astype("datetime64[ns, US/Eastern]")
            elif col_type == SqlTypeNames.TIMESTAMP:
                df[col] = pd.to_datetime(df[col], utc=True)
            logging.info(f"Column {col} has been transformed to {df[col].dtype}.")
        else:
            continue
    logging.info(f"Loading process for {len(records)} {records[0].__class__.__name__} to {table_id} started.")
    client = bigquery.Client()
    create_bq_table_if_not_exists(table_id, schema=schema)
    load_job_config = bigquery.LoadJobConfig(write_disposition=load_method)
    is_bq_query_done(client.load_table_from_dataframe(
        dataframe=df,
        destination=table_id,
        job_config=load_job_config
    ).result())
    logging.info(f"Loaded {len(records)} {records[0].__class__.__name__} to {table_id}.")


def get_bq_schema(o: dataclass, timestamp=False) -> Mapping[str, SqlTypeNames]:
    """
    This method returns the BQ schema for a dataclass object o. The
    BQ schema is returned as a mapping between the field name and the
    BQ data type.
    :param timestamp: If true, set BQ column type for datetime object to TIMESTAMP,DATETIME otherwise.
    """
    assert is_dataclass(obj=o)

    schema = {}
    for fld in fields(o.__class__):
        if fld.type == str:
            schema[fld.name] = SqlTypeNames.STRING
        elif fld.type == bytes:
            schema[fld.name] = SqlTypeNames.BYTES
        elif fld.type == int:
            schema[fld.name] = SqlTypeNames.INT64
        elif fld.type == float:
            schema[fld.name] = SqlTypeNames.FLOAT64
        elif fld.type == bool:
            schema[fld.name] = SqlTypeNames.BOOL
        elif fld.type == dict:
            schema[fld.name] = SqlTypeNames.STRUCT
        elif fld.type == date:
            schema[fld.name] = SqlTypeNames.DATE
        elif fld.type == datetime_time:
            schema[fld.name] = SqlTypeNames.TIME
        elif fld.type == datetime:
            if timestamp:
                schema[fld.name] = SqlTypeNames.TIMESTAMP
            else:
                schema[fld.name] = SqlTypeNames.DATETIME
        else:
            schema[fld.name] = SqlTypeNames.STRING
    return schema


def create_bq_table_if_not_exists(table_id: str, schema: Mapping[str, str]) -> None:
    """
    This helper function creates the table with table_id with the given schema in BigQuery
    if the table does not exist already.

    This helper function will wait until the table creation task to complete before it

    >>> t_id = "[GCP Project Name].[BigQuery Schema Name].[BigQuery Table Name]"
    >>> s = {"NAME OF A NUMERIC COLUMN": "NUMERIC",
    ...      "NAME OF A CHARACTER DATA TYPE COLUMN": "STRING",
    ...      "NAME OF A DATE/TIME COLUMN": "DATETIME"}
    >>> create_bq_table_if_not_exists(table_id=t_id, schema=s)

    :param table_id: The unique identifier of the table
    :param schema: The mapping between table name and data type.
    """
    # Create the SQL query to create the table with table id.
    table_creation_query = [f"CREATE TABLE IF NOT EXISTS `{table_id}`", "("]
    # Define the schema of the table.
    for col in schema:
        table_creation_query.append(col)
        table_creation_query.append(schema[col])
        table_creation_query.append(",")
    table_creation_query = table_creation_query[:-1]
    table_creation_query.append(")")
    table_creation_query.append(";")
    # Execute the table creation query, and wait for it to complete.
    is_bq_query_done(bigquery.Client().query(" ".join(table_creation_query)))


def create_table_from_gcs(
        bq_table_id, schema_str, **context) -> None:
    """
    This will insert the csv data into the temp table

    This needs:
    - schema_list [bigquery.SchemaField]
    - bq_table_id
    - List of csv files

    """
    bq_client = bigquery.Client()
    # todo: catch schema not matching issue
    logger.info(f"{'-' * 100}\nStart importing data to temp table")

    GCS_BUCKET_NAME = context["dag_run"].conf["bucket"]
    OBJECT_NAME = context["dag_run"].conf["name"]
    SCHEMA_LIST_BQ = text_to_bq_schema(schema_str)
    LIST_OF_FILES = [f"gs://{GCS_BUCKET_NAME}/{OBJECT_NAME}"]

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        schema=SCHEMA_LIST_BQ,
        skip_leading_rows=1,
    )

    bq_load_job = bq_client.load_table_from_uri(
        LIST_OF_FILES, f"{bq_table_id}", job_config=job_config
    )
    is_bq_query_done(bq_load_job)
    count_records_tbl(f"{bq_table_id}")
    logger.info(f"Importing data to temp table {bq_table_id} DONE")


def add_timestamp_column(bq_table_id, datetime_str):
    """
    Add the ingestion timestamp the bq table
    """
    logger.info("Altering the temp table to add timestamp column")
    run_bq_query(
        f"""alter table {bq_table_id} add column ingestion_timestamp STRING"""
    )
    logger.info("Updating the newly created columnn timestamp in  the temp table ")
    run_bq_query(
        f"""update {bq_table_id} set ingestion_timestamp = "{datetime_str}" where  TRUE"""
    )


def get_distinct_records(bq_table_id, dedup_cols):
    logger.info(
        f"Create and replace temp table to remove duplicate  on the basis of {dedup_cols} cols "
    )
    run_bq_query(
        f"""create or replace table {bq_table_id} as
        select * except (rn) from
        (select * , (row_number() over
            (partition by {' , '.join(dedup_cols)} )) as rn
            from {bq_table_id}
        )
        where rn = 1
        """
    )


def insert_temp_to_main(bq_target_table_id, bq_source_table_id, dedup_cols):
    """
    Insert the data from the temp table to the original table

    Steps:
    - Remove the records from the target table on the basis of dedup cols
    - Insert the records from the temp table (already deduped)
    """
    logger.info("Running the merge query to insert only the new records into the main table")

    logger.info("Remove the records if already exists for that key")
    run_bq_query(
        f"""
            MERGE {bq_target_table_id} AS main
            USING (
                SELECT
                    t.*,
                    TO_BASE64(MD5(CONCAT( {' , '.join(dedup_cols)} ))) AS cdc_hash,
                FROM
                    {bq_source_table_id} t ) AS temp
            ON
                main.cdc_hash = temp.cdc_hash
        WHEN MATCHED
                THEN DELETE
            """
    )

    logger.info("Inserting the values from the temp to main table")
    # Insert the records from the temp table
    run_bq_query(
        f"""
                INSERT INTO {bq_target_table_id} SELECT  * ,  TO_BASE64(MD5(CONCAT( {' , '.join(dedup_cols)}))) AS cdc_hash FROM {bq_source_table_id}
         """
    )


def is_bq_query_done(bq_query_job, credentials: impersonated_credentials = None, project: str = None):
    """ Checks if the query is Done and not in still running phase through an api call"""
    bq_client = bigquery.Client(credentials=credentials, project=project)
    while True:
        query_job = bq_client.get_job(
            bq_query_job.job_id,
            location=bq_query_job.location,
        )

        if query_job.state == "DONE":
            logger.info("Finished running the query. Check if it was executed without errors")
            break
        time.sleep(1)
        logger.info(
            "Job {} is currently in state {}".format(
                bq_query_job.job_id, query_job.state
            )
        )
    logger.info(f'job_id : {bq_query_job.job_id}')
    logger.info(f'location : {bq_query_job.location}')

    return True


def bg_query_success(bq_query_job, credentials: impersonated_credentials = None, project: str = None) -> None:
    """
    Checks if the BQ was complete and  executed without any errors
    """
    # check if the query is complete (==DONE)
    is_bq_query_done(bq_query_job, credentials, project)

    bq_client = bigquery.Client(credentials=credentials, project=project)
    query_job = bq_client.get_job(
        bq_query_job.job_id,
        location=bq_query_job.location,
    )

    # Check if there were any errors
    if query_job.error_result is not None:
        raise (Exception(query_job.error_result["message"]))
    else:
        logger.info("Query ran without errors.")


def count_records_tbl(table: str) -> None:
    """
    Check if >0 records loaded in the BQ table
    """
    bq_client = bigquery.Client()
    query = f"""select count(1) as cnt from {table}"""
    bq_query_job = bq_client.query(query)

    for row in bq_query_job:
        count_rows = row.cnt

    if count_rows > 0:
        logger.info(f"Successfully loaded {count_rows} rows in the {table} table")
    else:
        logger.info("Warning: 0 records were loaded")

    return count_rows


def run_bq_query(query, target_service_account: str = None, target_project: str = None, job_config: str = None):
    """
    Runs the bq job and checks if it was successful?
    """
    bq_client = bigquery.Client()
    if target_service_account:
        # Scopes required for BigQuery operations
        scopes = [
            consts.SCOPE_CLOUD_PLATFORM,
            consts.SCOPE_BIGQUERY
        ]
        # Create source credentials
        source_credentials, project = google.auth.default()
        project = target_project

        # Create impersonated credentials using the source credentials
        impersonated_creds = impersonated_credentials.Credentials(
            source_credentials=source_credentials,
            target_principal=target_service_account,
            target_scopes=scopes
        )
        impersonated_creds_str = str(impersonated_creds)
        logger.info(f'Impersonated Credentials : {impersonated_creds_str}')
        logger.info(f'Project : {project}')
        # Initialize the BigQuery client with impersonated credentials
        bq_client = bigquery.Client(credentials=impersonated_creds, project=project)
        bq_query_job = bq_client.query(query, job_config=job_config)
        bg_query_success(bq_query_job, impersonated_creds, project)
        bq_query_result = bq_query_job.result()
        logger.info(f'total_rows : {bq_query_result.total_rows}')

        return bq_query_job

    bq_query_job = bq_client.query(query, job_config=job_config)
    bg_query_success(bq_query_job)

    return bq_query_job


def run_bq_query_with_params(sql_query: str, job_config: Optional[QueryJobConfig] = None) -> QueryJob:
    """
    Executes a BigQuery SQL query with an optional job configuration.

    :param sql_query: The SQL query string to execute.
    :param job_config: Optional QueryJobConfig for configuring the query.
    :return: The BigQuery QueryJob object.
    """
    client = bigquery.Client()

    query_job = client.query(sql_query, job_config=job_config)
    bg_query_success(query_job)

    return query_job


def drop_bq_table(bq_table_id):
    """
    Drop the table
    """
    run_bq_query(f"""drop table if exists {bq_table_id}""")


def create_main_table_if_not_exists(bq_table_id, schema_text):
    """
    Create the main table if not already existing
    """
    logger.info(f"{'-' * 100}\nCreate the main table if not exists")
    schema_text += " , ingestion_timeestamp STRING , cdc_hash STRING"
    query = f"create table if not exists {bq_table_id} ({schema_text})"
    run_bq_query(query)


def get_blobs(gcs_bucket_name, prefix):
    """
    Get the list of objects to be copied - default csv at the moment
    """

    client = storage.Client()
    list_of_files = []
    for blob in client.list_blobs(gcs_bucket_name, prefix=prefix):
        file = f"gs://{gcs_bucket_name}/{blob.name}"
        logger.info(file)
        list_of_files.append(file)

    return [i for i in list_of_files if "csv" in i]


def text_to_bq_schema(schema_text):
    """
    Takes the text schema and converts it into schema object for using it through python sql query
    Example
    - Input : (ClientID STRING , SendID STRING)
    - Output : [bigquery.SchemaField("ClientID", "STRING"), bigquery.SchemaField("SendID", "STRING")]
    """

    temp_schema_list = [
        i.strip().split(" ") for i in schema_text.replace("\n", "").split(",")
    ]
    return [bigquery.SchemaField(i[0], i[1]) for i in temp_schema_list]


def load_gcs_to_bq_parquet(table_name, project_id, dataset_name, gcs_table_staging_dir):
    """
    Loads the parquet files in gcs to big query table (single table)
    """
    logging.info(f"Starting import of data for table {table_name} from {gcs_table_staging_dir}")
    bq_client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.PARQUET
    )
    bq_table_id = f"{project_id}.{dataset_name}.{table_name}"
    bq_load_job = bq_client.load_table_from_uri(
        gcs_table_staging_dir,
        bq_table_id,
        job_config=job_config
    )
    bq_load_job.result()

    target_table = bq_client.get_table(bq_table_id)
    logging.info(
        f"Finished import of data for table {table_name}: "
        f"Imported from GCS Bucket - {gcs_table_staging_dir} to BQ Table name {bq_table_id}. Imported {target_table.num_rows} rows.")


def bulk_load_gcs_to_bq_parquet(gcs_bucket_name, gcs_dir_prefix, project_id, dataset_name, job_dir):
    """
    Bulk load of the parquet data from the gcs bucket to bq tables (Uses the same table name as that of the folder in gcs bucket)
    """

    logging.info("Starting import of data for all tables from GCS into BigQuery")

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(gcs_bucket_name)
    blobs_iterator = bucket.list_blobs(prefix=f"{gcs_dir_prefix}/", delimiter="/")
    response = blobs_iterator._get_next_page_response()

    for prefix in response['prefixes']:
        start_index = prefix.index(job_dir) + len(job_dir) + 1
        end_index = prefix.index("/", start_index)
        table_name = prefix[start_index:end_index]

        gcs_table_staging_dir = f"gs://{gcs_bucket_name}/{prefix}*.parquet"

        load_gcs_to_bq_parquet(table_name, project_id, dataset_name, gcs_table_staging_dir)

    logging.info("Finished import of data for all tables from GCS into BigQuery")
    return "GCS to BigQuery load task completed for all tables"


def bulk_load_tables_task(gcs_bucket_name, gcs_staging_dir, project_id, dataset_name, job_dir):
    """
    Airflow task for loading the bulk tables to big query:
    """
    logging.info(f"""Arguments
        gcs_bucket_name = {gcs_bucket_name}
        gcs_staging_dir = {gcs_staging_dir}
        project_id= {project_id}
        dataset_name = {dataset_name}""")

    gcs_dir_prefix = gcs_staging_dir[(gcs_staging_dir.index(gcs_bucket_name) + len(gcs_bucket_name) + 1):]
    bulk_load_gcs_to_bq_parquet(gcs_bucket_name, gcs_dir_prefix, project_id, dataset_name, job_dir)


def get_table_stats(table_name, project_id, dataset_name):
    bq_client = bigquery.Client()
    query = f"""select * from {project_id}.{dataset_name}.{table_name} limit 1"""
    bq_query_job = bq_client.query(query)

    for row in bq_query_job:
        logging.info(f"Sample row for table {project_id}.{dataset_name}.{table_name}\n", row)


def bulk_check_bq_tables(gcs_bucket_name, gcs_dir_prefix, project_id, dataset_name, job_dir):
    """
    Bulk check the tables with top 1 rows
    """

    logging.info("Starting to check if the import in the BQ was successful")

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(gcs_bucket_name)
    blobs_iterator = bucket.list_blobs(prefix=f"{gcs_dir_prefix}/", delimiter="/")
    response = blobs_iterator._get_next_page_response()

    for prefix in response['prefixes']:
        start_index = prefix.index(job_dir) + len(job_dir) + 1
        end_index = prefix.index("/", start_index)
        table_name = prefix[start_index:end_index]

        get_table_stats(table_name, project_id, dataset_name)

    logging.info("Finished import of data for all tables from GCS into BigQuery")
    return "GCS to BigQuery load task completed for all tables"


def bulk_load_tables_check_task(gcs_bucket_name, gcs_staging_dir, project_id, dataset_name, job_dir):
    """
    Task to check if the load was successful
    """
    logging.info(f"""Arguments
        gcs_bucket_name = {gcs_bucket_name}
        gcs_staging_dir = {gcs_staging_dir}
        project_id= {project_id}
        dataset_name = {dataset_name}""")

    gcs_dir_prefix = gcs_staging_dir[(gcs_staging_dir.index(gcs_bucket_name) + len(gcs_bucket_name) + 1):]
    bulk_check_bq_tables(gcs_bucket_name, gcs_dir_prefix, project_id, dataset_name, job_dir)


def check_bq_table_exists(table_ref: str):
    """
    Checks if the table exists in the bq already
    returns: True if the table exists

    :param table_ref:
    """
    bq_client = bigquery.Client()
    return table_exists(bq_client, table_ref)


def table_exists(bigquery_client, table_ref: str):
    try:
        bigquery_client.get_table(table_ref)  # Make an API request.
        logging.info(f"Table {table_ref} is found.")
        return True
    except NotFound:
        logging.info(f"Table {table_ref} is not found.")
        return False


def submit_transformation(bigquery_client, view_id, view_ddl):
    logger.info(view_ddl)
    bigquery_client.query(view_ddl).result()

    view = bigquery_client.get_table(view_id)
    view_schema = view.schema
    columns = consts.COMMA_SPACE.join([field.name for field in view_schema])

    return {
        consts.ID: view_id,
        consts.COLUMNS: columns
    }


def create_external_table(bigquery_client, ext_table_id: str, file_uri: str, expiration_hours=6, file_format='PARQUET', field_delimiter=None, skip_leading_rows=None, table_schema=None):
    """
    Create external table from files in GCS.

    :param bigquery_client: BigQuery client instance
    :param ext_table_id: External table ID in format project.dataset.table
    :param file_uri: GCS URI(s) for the file(s), can be a single URI or pattern (e.g., 'gs://bucket/file.csv' or 'gs://bucket/*.csv')
    :param expiration_hours: Number of hours until table expires (default: 6)
    :param file_format: File format - 'PARQUET', 'CSV', 'JSON', etc. (default: 'PARQUET')
    :param field_delimiter: Delimiter for CSV file format only
    :param skip_leading_rows: Number of leading rows to skip (CSV only). Default to None.
    :param table_schema: Table Schema to use to create external table (default: None)
    :return: Dictionary with table ID and columns
    """
    # Build options list
    options = [
        f"format = '{file_format}'",
        f"uris = ['{file_uri}']",
        f"expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {expiration_hours} HOUR)"
    ]

    # Add skip_leading_rows option for CSV format
    if skip_leading_rows is not None:
        options.append(f"skip_leading_rows = {skip_leading_rows}")

    if field_delimiter is not None:
        options.append(f"field_delimiter = '{field_delimiter}'")

    options_str = ',\n                '.join(options)

    # Build DDL with or without schema
    if table_schema is not None:
        ext_table_ddl = f"""
            CREATE OR REPLACE EXTERNAL TABLE
                `{ext_table_id}`
            (
                {table_schema}
            )
            OPTIONS (
                {options_str}
            );
        """
    else:
        ext_table_ddl = f"""
            CREATE OR REPLACE EXTERNAL TABLE
                `{ext_table_id}`
            OPTIONS (
                {options_str}
            );
        """

    return submit_transformation(bigquery_client, ext_table_id, ext_table_ddl)


def get_table_columns(bigquery_client, table_id):
    table = bigquery_client.get_table(table_id)
    table_schema = table.schema
    columns = consts.COMMA_SPACE.join([field.name for field in table_schema])

    return {
        consts.ID: table_id,
        consts.COLUMNS: columns
    }


def create_backup_table(backup_table_id: str, source_table_id: str):
    """
    This method backs up a provided source table.

    :param backup_table_id: Back table id in the following format: project_id.dataset_id.table_name
    :param source_table_id: Source table id in the following format: project_id.dataset_id.table_name
    """
    backup_table_ddl = f"""
            CREATE OR REPLACE TABLE
                `{backup_table_id}`
            CLONE
                `{source_table_id}`
            OPTIONS (
                expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 14 DAY),
                description = "Backup Table, source table id: {source_table_id}"
            );
        """
    return submit_transformation(bigquery.Client(), backup_table_id, backup_table_ddl)


def apply_row_transformation(bigquery_client, source_table: dict, filter_rows: List = None, expiration_hours=6):
    if filter_rows:
        filter_conditions = ' AND '.join([fltr for fltr in filter_rows])
    else:
        filter_conditions = 'true'
    logger.info(f'filter_conditions: {filter_conditions}')

    source_table_id = source_table.get(consts.ID)

    row_transform_view_id = f'{source_table_id}_ROW_TF'
    row_transform_view_ddl = f"""
        CREATE OR REPLACE VIEW
            `{row_transform_view_id}`
        OPTIONS (
            expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {expiration_hours} HOUR),
            description = "View for row transformation"
        ) AS
            SELECT *
            FROM `{source_table_id}`
            WHERE {filter_conditions}
    """

    return submit_transformation(bigquery_client, row_transform_view_id, row_transform_view_ddl)


def apply_column_transformation(bigquery_client, source_table: dict, add_columns=None,
                                drop_columns=None, expiration_hours=6):
    exclude = drop_columns or []

    if exclude == '*':
        custom_columns = []
    else:
        custom_columns = [column_name for column_name in source_table.get(consts.COLUMNS).split(consts.COMMA_SPACE) if
                          column_name not in exclude]

    if add_columns:
        custom_columns = custom_columns + add_columns

    custom_column_str = consts.COMMA_SPACE.join(custom_columns)
    source_table_id = source_table.get(consts.ID)

    columns_transform_view_id = f'{source_table_id}{consts.COLUMN_TRANSFORMATION_SUFFIX}'
    columns_transform_view_ddl = f"""
        CREATE OR REPLACE VIEW
            `{columns_transform_view_id}`
        OPTIONS (
            expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {expiration_hours} HOUR),
            description = "View for column transformation"
        ) AS
            SELECT  {custom_column_str}
            FROM `{source_table_id}`;
    """

    return submit_transformation(bigquery_client, columns_transform_view_id, columns_transform_view_ddl)


def map_names(names: list, mappings: dict):
    return [mappings.get(x) if x in mappings else x for x in names]


def map_table_columns(bigquery_client, table_id: str, name_mappings: dict):
    table = bigquery_client.get_table(table_id)
    columns = [field.name for field in table.schema]
    return consts.COMMA_SPACE.join(map_names(columns, name_mappings))


def convert_timestamp_to_datetime(bigquery_client, source_table_id: str, column_mappings: dict = None, tz: str = consts.TORONTO_TIMEZONE_ID):
    table = bigquery_client.get_table(source_table_id)
    schema = table.schema
    columns = []
    logger.info(f'Got timezone input : {tz}')
    for field in schema:
        name = field.name
        type = field.field_type
        alias = name
        mapped_name = name

        # handle renaming for view_creator
        if column_mappings and (name in column_mappings):
            alias = column_mappings.get(name)
            mapped_name = f'{name} AS {alias}'

        # handle timestamp conversion
        if type == 'TIMESTAMP':
            if tz.upper() == 'ODS':
                mapped_name = f"DATETIME({name}) AS {alias}"
            else:
                mapped_name = f"DATETIME({name}, '{tz}') AS {alias}"

        columns.append(mapped_name)

    return consts.COMMA_SPACE.join(columns)


def apply_timestamp_transformation(bigquery_client, source_table: dict, expiration_hours=6, tz: str = consts.TORONTO_TIMEZONE_ID):
    # logger.info(f'Got timezone input : {tz}')
    source_table_id = source_table.get(consts.ID)
    timestamp_transform_view_columns = convert_timestamp_to_datetime(bigquery_client, source_table_id, tz=tz)

    timestamp_transform_view_id = f'{source_table_id}_TIMESTAMP_TF'
    timestamp_transform_view_ddl = f"""
        CREATE OR REPLACE VIEW
            `{timestamp_transform_view_id}`
        OPTIONS (
            expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {expiration_hours} HOUR),
            description = "View for timestamp transformation"
        ) AS
            SELECT  {timestamp_transform_view_columns}
            FROM `{source_table_id}`;
    """

    return submit_transformation(bigquery_client, timestamp_transform_view_id, timestamp_transform_view_ddl)


def apply_join_transformation(bigquery_client, source_table: dict, join_spec: dict, expiration_hours=6):
    source_table_id = source_table.get(consts.ID)
    join_transform_view_id = f"{source_table_id}_JOIN_TF"

    source = f'`{source_table_id}` s {join_spec.get(consts.JOIN_CLAUSE)}'
    join_transform_view_ddl = f"""
                CREATE OR REPLACE VIEW
                    `{join_transform_view_id}`
                OPTIONS (
                  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {expiration_hours} HOUR),
                  description = "View for join transformation"
                )
                AS
                    SELECT {join_spec.get(consts.COLUMNS)}
                    FROM {source};
            """

    return submit_transformation(bigquery_client, join_transform_view_id, join_transform_view_ddl)


def apply_execution_id_transformation(bigquery_client, source_table: dict, execution_id: str, expiration_hours=6):
    source_table_id = source_table.get(consts.ID)
    source_table_columns = source_table.get(consts.COLUMNS)
    result_columns = f"{source_table_columns}, {execution_id} AS EXECUTION_ID"

    exec_id_transform_view_id = f"{source_table_id}_EXEC_ID_TF"

    exec_id_transform_view_ddl = f"""
                CREATE OR REPLACE VIEW
                    `{exec_id_transform_view_id}`
                OPTIONS (
                  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {expiration_hours} HOUR),
                  description = "View for execution id transformation"
                )
                AS
                    SELECT {result_columns}
                    FROM {source_table_id};
            """

    return submit_transformation(bigquery_client, exec_id_transform_view_id, exec_id_transform_view_ddl)


def apply_schema_sync_transformation(bigquery_client: object, source_table: dict, target_table_id: str,
                                     expiration_hours: object = 6) -> object:
    source_table_id = source_table.get(consts.ID)
    table = bigquery_client.get_table(target_table_id)
    table_schema = table.schema
    columns = consts.COMMA_SPACE.join([field.name for field in table_schema])

    schema_sync_transform_view_id = f"{source_table_id}_SCHEMA_SYNC_TF"

    schema_sync_transform_view_ddl = f"""
                CREATE OR REPLACE VIEW
                    `{schema_sync_transform_view_id}`
                OPTIONS (
                  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {expiration_hours} HOUR),
                  description = "View for schema transformation"
                )
                AS
                    SELECT {columns}
                    FROM {source_table_id};
            """

    return submit_transformation(bigquery_client, schema_sync_transform_view_id, schema_sync_transform_view_ddl)


def get_execution_id(bigquery_client, file_name, dag_id, env):
    if Variable is not None:
        var_dict = Variable.get(dag_id, deserialize_json=True, default_var={})
    else:
        var_dict = {}
    load_result_table = f"pcb-{env}-landing.domain_customer_acquisition.LOAD_RESULT"
    ret_exec_id = None
    if var_dict and var_dict.get('execution_id', {}):
        execution_id = int(var_dict.get('execution_id'))
        if execution_id <= consts.EXECUTION_ID_LOWER_BOUND:
            raise AirflowException(
                f"Provided execution_id: {execution_id} is not greater than {consts.EXECUTION_ID_LOWER_BOUND}.")
        var_dict_exec_id_add_query = f"""
            INSERT INTO `{load_result_table}` (TABLE_NM, LOAD_ERRORS, REC_LOAD_DT_TM_STAMP, FILE_CREATE_DT, EXECUTION_ID, FILENAME, REC_CNT)
            VALUES ('LOAD_RESULT', 0, CURRENT_DATETIME('America/Toronto'), CURRENT_DATE(), {execution_id}, '{file_name}', 1)
        """
        bigquery_client.query(var_dict_exec_id_add_query).result()
        ret_exec_id = execution_id
    else:
        new_exec_id_add_query = f"""
            INSERT INTO `{load_result_table}` (TABLE_NM, LOAD_ERRORS, REC_LOAD_DT_TM_STAMP, FILE_CREATE_DT, EXECUTION_ID, FILENAME, REC_CNT)
            SELECT 'LOAD_RESULT', 0, CURRENT_DATETIME('America/Toronto'), CURRENT_DATE(), max(EXECUTION_ID), '{file_name}', 1 FROM
            (select max(EXECUTION_ID)+1 as EXECUTION_ID FROM `{load_result_table}`
            UNION ALL
            select 1000000)
        """
        bigquery_client.query(new_exec_id_add_query).result()
        get_added_execution_id_query = f"""
            SELECT max(EXECUTION_ID) as EXECUTION_ID FROM `{load_result_table}`
            WHERE FILENAME = '{file_name}'
        """
        results = bigquery_client.query(get_added_execution_id_query).result().to_dataframe()
        ret_exec_id = int(results["EXECUTION_ID"].values[0])
    ret_exec_id_unique_query = f"""
        SELECT count(EXECUTION_ID) as cnt FROM `{load_result_table}`
        WHERE EXECUTION_ID = {ret_exec_id}
    """
    results = bigquery_client.query(ret_exec_id_unique_query).result().to_dataframe()
    if int(results["cnt"].values[0]) > 1:
        raise AirflowException(f"Generated EXECUTION_ID {ret_exec_id} is not unique.")
    return ret_exec_id


def parse_join_config(bq_client, join_config, bq_table_id, add_output_prefix=True, join_type='JOIN'):
    if not join_config:
        return
    logger.info(f'bq_table_id: {bq_table_id}')
    if add_output_prefix:
        output_columns = consts.COMMA_SPACE.join([f't.{col}' for col in join_config.get(consts.OUTPUT_COLUMNS)])
    else:
        output_columns = consts.COMMA_SPACE.join([f'{col}' for col in join_config.get(consts.OUTPUT_COLUMNS)])
    bq_table_columns = f'{output_columns}, s.*'
    logger.info(f'bq_table_columns: {bq_table_columns}')

    if table_exists(bq_client, bq_table_id):
        result_df = bq_client.query(f'SELECT * FROM `{bq_table_id}` LIMIT 1;').to_dataframe()
        columns = list(result_df.columns)
        logger.info(f'columns: {columns}')
        bq_table_columns = \
            f'{output_columns},' + \
            consts.COMMA_SPACE.join([f's.{c}' for c in columns if c not in join_config.get(consts.OUTPUT_COLUMNS)])

    join_table_name = join_config.get(consts.TABLE_NAME)
    join_table_project_id = join_config.get(consts.PROJECT_ID)
    join_table_dataset_id = join_config.get(consts.DATASET_ID)

    if join_table_project_id and join_table_dataset_id:
        bq_join_table_id = f'{join_table_project_id}.{join_table_dataset_id}.{join_table_name}'
    else:
        destination_dataset_id, destination_table_id = bq_table_id.rsplit('.', 1)
        bq_join_table_id = f"{destination_dataset_id}.{join_table_name}"

    join_col = join_config.get(consts.JOIN_COLUMNS)
    join_columns = ' AND '.join([f"{col.get('from_col')}={col.get('to_col')}" for col in join_col])
    if join_table_name is None or join_columns is None:
        raise Exception("Invalid join configuration, join_table_name and join_columns are mandatory!")

    filters = join_config.get('filters')
    if filters is not None:
        logger.info(filters)
        filter_conditions = ' '.join([f" AND {fltr}" for fltr in filters])
        logger.info(filter_conditions)
        join_clause = f'{join_type} `{bq_join_table_id}` t ON {join_columns} {filter_conditions}'
    else:
        join_clause = f'{join_type} `{bq_join_table_id}` t ON {join_columns}'

    return {
        consts.COLUMNS: bq_table_columns,
        consts.JOIN_CLAUSE: join_clause
    }


def get_table_labels(table_id: str):
    bq_client = bigquery.Client()
    bq_table_labels = {}
    table = bq_client.get_table(table_id)
    if table.labels:
        bq_table_labels = table.labels
        logger.info(f"Table ID: {table_id}.")
        logger.info(f"table_labels: {bq_table_labels}.")
    return bq_table_labels


def create_or_replace_table(table: Union[str, Table], schema_path: str = None, partitioning_column=None,
                            partition_type=None):
    """
    Function to create an empty table. Schema can be provided.
    If table exists, the table will be replaced by an empty table.
    If partitioning column is provided, the method will create partitioned table.

    Args:
        table (Union[str, google.cloud.bigquery.table.Table]):
            The table to create. Can pass either fully qualified table id or Table with params.
        schema_path (str): The local file path within repository containing JSON schema file.
    """
    client = bigquery.Client()

    if isinstance(table, str):
        _table = Table(table_ref=table)
    else:
        _table = table

    schema = client.schema_from_json(schema_path)

    # delete table, ignores errors if not found
    client.delete_table(
        table=_table,
        not_found_ok=True
    )

    # add schema to table option
    _table.schema = schema

    # Apply partitioning on the table
    if (partitioning_column is not None):
        if (partition_type not in [bigquery.TimePartitioningType.MONTH, bigquery.TimePartitioningType.DAY,
                                   bigquery.TimePartitioningType.HOUR, bigquery.TimePartitioningType.YEAR]):
            raise AirflowException("Invalid granularity for TimePartitioningType. Failing the DAG.")
        _table.time_partitioning = bigquery.TimePartitioning(
            type_=partition_type,
            field=partitioning_column  # name of column to use for partitioning
        )

    # create table
    client.create_table(
        table=_table,
        exists_ok=True
    )

# ---------------------------------------------
# Read SQL file
# ---------------------------------------------


def read_sql_file(sql_file_path):
    gcp_config = read_variable_or_file(consts.GCP_CONFIG)
    deploy_env = (
        gcp_config.get(consts.DEPLOYMENT_ENVIRONMENT_NAME)
        or os.environ.get(consts.DEPLOYMENT_ENVIRONMENT_NAME)
        or "dev"
    )
    sql_file = f'{DAGS_FOLDER}/{sql_file_path}'
    if os.path.exists(sql_file):
        with open(sql_file, 'r') as f:
            content = f.read()
            if deploy_env is not None and consts.ENV_PLACEHOLDER in content:
                content = content.replace(consts.ENV_PLACEHOLDER, deploy_env)
            return content
    else:
        logger.error(f'{sql_file} does not exist')
        raise (Exception(f'{sql_file} does not exist'))

# ---------------------------------------------
# Load the data in BQ table using SQL Script
# ---------------------------------------------


def resolve_placeholders(content: str, context: dict):
    for key in context.keys():
        context_var_palce_holder = f"{{{key}}}"
        if context_var_palce_holder in content:
            context_val = context[key]
            logger.info(f"context_variable:{key} Value:{context_val}")
            content = content.replace(context_var_palce_holder, context_val)
    return content


def run_bq_script(
        sql_script_path,
        context):
    sql_script = read_sql_file(sql_script_path)

    sql_script = resolve_placeholders(sql_script, context)
    logger.info(f"sql_script:{sql_script}")

    client = bigquery.Client()
    query_job = client.query(sql_script)

    query_job.result()

    if query_job.done() and query_job.error_result is None:
        logger.info("Query job completed successfully.")
    else:
        logger.error(f"BQ select failed:${query_job.errors}\nsql:${sql_script}")

############################################################
# Create Ref Tables Schema
############################################################


def create_schema_from_file(schema_file_path, delimiter):
    if os.path.exists(schema_file_path):
        with open(schema_file_path, 'r') as f:
            contents = f.readlines()
            schema = []
            for lines in contents:
                split_rec = lines.strip('\n').split(delimiter)
                schema.append(bigquery.SchemaField(split_rec[0], split_rec[1]))
        return schema
    else:
        logger.error(f"{schema_file_path} does not exist")
        raise (Exception(f"{schema_file_path} does not exist"))


def build_back_up_job(table_ref: str, backup_enabled: bool = False):
    if backup_enabled:
        backup_table_ref = table_ref + consts.BACK_UP_STR
        logger.info(f'Started creating {backup_table_ref}')
        create_backup_table(backup_table_ref, table_ref)
        logger.info(f'Finished creating {backup_table_ref}')
    else:
        logger.info('Skipping backup as backup is disabled.')


def copy_table(source_table_ref: str, target_table_ref: str) -> None:
    """
    Function to create a copy of a table. This function copies both data and the structure of the source table.
    :param source_table_ref:
    :param target_table_ref:
    :return: None
    """
    table_copy_stmt = f"""
            CREATE OR REPLACE TABLE
                `{target_table_ref}`
            LIKE `{source_table_ref}`
            AS
                SELECT * FROM `{source_table_ref}`;
    """

    logger.info(table_copy_stmt)
    bigquery.Client().query(table_copy_stmt).result()
    logger.info(f'Finished copying {source_table_ref} to {target_table_ref}')


def apply_deduplication_transformation(bigquery_client, source_table: dict, dedup_columns: list, expiration_hours=6):
    """
    Apply deduplication transformation using ROW_NUMBER() window function.
    Args:
        bigquery_client: BigQuery client instance
        source_table: Source table/view information
        dedup_columns: List of columns to use for partitioning in ROW_NUMBER()
        expiration_hours: View expiration time in hours
    Returns:
        dict: Transformed table information with new view ID
    """
    if not dedup_columns:
        return source_table

    source_table_id = source_table.get(consts.ID)
    dedup_transform_view_id = f"{source_table_id}_DEDUP_TF"

    dedup_transform_view_columns = source_table.get(consts.COLUMNS)

    # Build partition by clause
    partition_by_clause = ", ".join(dedup_columns)

    dedup_transform_view_ddl = f"""
                CREATE OR REPLACE VIEW
                    `{dedup_transform_view_id}`
                OPTIONS (
                  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {expiration_hours} HOUR),
                  description = "View for deduplication transformation"
                )
                AS
                    WITH temp_tab AS (
                        SELECT
                            ROW_NUMBER() OVER (PARTITION BY {partition_by_clause}) as rn,
                            {dedup_transform_view_columns}
                        FROM `{source_table_id}`
                    )
                    SELECT {dedup_transform_view_columns}
                    FROM temp_tab
                    WHERE rn = 1;
            """

    return submit_transformation(bigquery_client, dedup_transform_view_id, dedup_transform_view_ddl)


def schema_preserving_load(create_ddl_statement: str, insert_statement: str, target_table_id: str, bq_client=None) -> None:
    """
    Refreshes a BigQuery table by choosing the appropriate method based on table existence.
    - If table exists: Uses TRUNCATE + INSERT (preserves policy tags automatically)
    - If table doesn't exist: Uses CREATE OR REPLACE TABLE (the provided DDL statement)

    Args:
        create_ddl_statement: SQL CREATE OR REPLACE TABLE statement (used when table doesn't exist)
        insert_statement: SQL INSERT INTO statement (used when table exists)
        target_table_id: Fully-qualified table ID (project.dataset.table)
        bq_client: Optional BigQuery client. If not provided, a new client will be created.
    """
    client = bq_client if bq_client is not None else bigquery.Client()

    if table_exists(client, target_table_id):
        # Table exists - use TRUNCATE + INSERT (preserves policy tags automatically)
        logger.info(f"""Table {target_table_id} exists.
                    Using TRUNCATE + INSERT to preserve policy tags.
                    Truncating table: {target_table_id}""")

        # Step 1: Truncate table (preserves schema & policy tags)
        truncate_stmt = f"TRUNCATE TABLE `{target_table_id}`"
        logger.info(f"truncate_ddl: {truncate_stmt}")
        client.query_and_wait(truncate_stmt)

        # Step 2: Insert new data
        logger.info(f"""Inserting data into: {target_table_id}
                    Insert_statement: {insert_statement}""")
        client.query_and_wait(insert_statement)
        logger.info(f"Successfully refreshed {target_table_id} using TRUNCATE + INSERT (policy tags preserved)")
    else:
        # Table doesn't exist - use CREATE OR REPLACE
        logger.info(f"""Table {target_table_id} doesn't exist. Creating new table.
                    create_ddl_statement: {create_ddl_statement}""")
        client.query_and_wait(create_ddl_statement)
        logger.info(f"Successfully created {target_table_id} using CREATE OR REPLACE TABLE")


# ============================================================
# BigQuery REST API Utilities
# ============================================================
# These functions use REST API directly instead of SDK.
# Use when SDK has limitations (e.g., preserving schema fields
# like defaultValueExpression that SDK might drop, or when you
# need fine-grained control over table metadata updates).

_BQ_API_BASE = "https://bigquery.googleapis.com/bigquery/v2"
_BQ_REST_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
_BQ_REST_REQUEST_TIMEOUT_SECONDS = 30


class BigQueryRestApiError(Exception):
    """Raised when a BigQuery REST API request fails (non-2xx, excluding 404)."""

    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


def _get_bq_rest_credentials() -> Tuple[Any, str]:
    """
    Get authenticated credentials for BigQuery REST API.

    Internal helper function for REST API operations.

    Returns:
        Tuple of (credentials, project_id) for use with REST API requests.

    Raises:
        RuntimeError: If requests library is not available.
    """
    if requests is None:
        raise RuntimeError("requests library is required for REST API operations.")
    credentials, project_id = google.auth.default(scopes=_BQ_REST_SCOPES)
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    return credentials, project_id


def parse_table_fqdn(table_fqdn: str) -> Tuple[str, str, str]:
    """
    Parse fully qualified table name into components.

    Args:
        table_fqdn: Fully qualified table name in format "project.dataset.table".

    Returns:
        Tuple of (project_id, dataset_id, table_id).

    Raises:
        ValueError: If table_fqdn format is invalid.
    """
    parts = table_fqdn.split(".", 2)
    if len(parts) != 3:
        raise ValueError(
            f"table_fqdn must be 'project.dataset.table', got: {table_fqdn!r}"
        )
    return parts[0], parts[1], parts[2]


def _bq_table_rest_url(project_id: str, dataset_id: str, table_id: str) -> str:
    """
    Build BigQuery REST API URL for a table resource.

    Internal helper function for REST API operations.

    Args:
        project_id: GCP project ID.
        dataset_id: BigQuery dataset ID.
        table_id: BigQuery table ID.

    Returns:
        Full REST API URL for the table resource.
    """
    return f"{_BQ_API_BASE}/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"


def make_bq_api_request(method: str, url: str, **kwargs: Any) -> Any:
    """
    Make authenticated HTTP request to BigQuery REST API.

    Args:
        method: HTTP method (GET, POST, PATCH, etc.).
        url: Full REST API URL.
        **kwargs: Additional arguments passed to requests.request().

    Returns:
        requests.Response object.

    Raises:
        RuntimeError: If requests library is not available.
        BigQueryRestApiError: If request fails (non-2xx status).
    """
    if requests is None:
        raise RuntimeError("requests library is required for REST API operations.")
    credentials, _ = _get_bq_rest_credentials()
    headers = {
        "Authorization": f"Bearer {credentials.token}",
        "Content-Type": "application/json",
    }
    extra_headers = kwargs.pop("headers", None)
    if extra_headers:
        headers.update(extra_headers)
    return requests.request(
        method,
        url,
        headers=headers,
        timeout=_BQ_REST_REQUEST_TIMEOUT_SECONDS,
        **kwargs,
    )


def get_table_via_rest(table_fqdn: str) -> Dict[str, Any]:
    """
    Get table resource via BigQuery REST API.

    This method preserves all table metadata fields that might be dropped
    by the SDK (e.g., defaultValueExpression).

    Args:
        table_fqdn: Fully qualified table name (project.dataset.table).

    Returns:
        Table resource as dictionary (JSON response from API).

    Raises:
        NotFound: If table does not exist.
        BigQueryRestApiError: If API request fails.
        RuntimeError: If requests library is not available.
    """
    project_id, dataset_id, table_id = parse_table_fqdn(table_fqdn)
    url = _bq_table_rest_url(project_id, dataset_id, table_id)
    response = make_bq_api_request("GET", url)
    if response.status_code == 404:
        raise NotFound("Table not found")
    if not response.ok:
        raise BigQueryRestApiError(
            f"BigQuery API request failed with status {response.status_code}",
            status_code=response.status_code,
        )
    return response.json()


def patch_table_via_rest(
    table_fqdn: str,
    body: Dict[str, Any],
    etag: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Patch table resource via BigQuery REST API.

    This method allows updating table metadata (e.g., clustering, description)
    while preserving other fields that SDK might drop.

    Args:
        table_fqdn: Fully qualified table name (project.dataset.table).
        body: Dictionary containing fields to update (will be JSON-serialized).
        etag: Optional etag for conditional updates (prevents concurrent modifications).

    Returns:
        Updated table resource as dictionary (JSON response from API).

    Raises:
        BigQueryRestApiError: If API request fails.
        RuntimeError: If requests library is not available.
    """
    project_id, dataset_id, table_id = parse_table_fqdn(table_fqdn)
    url = _bq_table_rest_url(project_id, dataset_id, table_id)
    headers = {"If-Match": etag} if etag else None
    response = make_bq_api_request("PATCH", url, json=body, headers=headers)
    if not response.ok:
        raise BigQueryRestApiError(
            f"BigQuery API request failed with status {response.status_code}",
            status_code=response.status_code,
        )
    return response.json()


def run_bq_query_inline(query, job_config: str = None):
    """
    Runs the bq job and checks if it was successful using blocking result() method.
    On Query execution errors, result() raises the underlying exception
    """
    bq_client = bigquery.Client()
    try:
        job = bq_client.query(query, job_config=job_config)
        return job.result()
    except Exception as e:
        logger.error(f"BigQuery job failed for query: {query} with exception {e}")
        raise
