import logging
import os
import pendulum
import json

from datetime import datetime, timedelta

from airflow import settings
from airflow.models.dagrun import DagRun
from airflow.exceptions import AirflowFailException
from airflow.utils.state import State
from google.cloud import bigquery
from google.cloud.bigquery.enums import WriteDisposition, DestinationFormat
from util.miscutils import read_variable_or_file, read_env_filepattern, read_yamlfile_env_suffix
from util.bq_utils import count_records_tbl, run_bq_query
from util.gcs_utils import delete_folder, delete_blobs, upload_blob, compose_file, list_blobs_with_prefix, copy_blob

logger = logging.getLogger(__name__)

############################################################
# Configs and constants
############################################################

ENV_PLACEHOLDER = '{env}'

# Config files
GCP_CONFIG = read_variable_or_file('gcp_config')
DEPLOY_ENV = GCP_CONFIG['deployment_environment_name']
DEPLOY_ENV_SUFFIX = GCP_CONFIG['deploy_env_storage_suffix']

# GCS Bucket / Directory (with dates)
TORONTO_TZ = pendulum.timezone('America/Toronto')
CURRENT_DATETIME = datetime.now(tz=TORONTO_TZ)
JOB_DATE = f"{CURRENT_DATETIME.strftime('%Y%m%d')}"
JOB_DATE_TIME = f"{CURRENT_DATETIME.strftime('%Y%m%d%H%M%S')}"

# BQ Settings
CURATED_PROJECT_ID = GCP_CONFIG.get('curated_zone_project_id')

# Others
REGION = "northamerica-northeast1"
TRIG_RULE = 'all_success'
TAGS = ["team-defenders-alerts", "outbound", "AML"]

# Index names
STAGING_BUCKET = 'staging_bucket'
STAGING_FOLDER = 'staging_folder'
SOURCE_HEADERS_FILE_PATH = 'headers_source_file_path'
OUTBOUND_BUCKET = 'outbound_bucket'
OUTBOUND_FILENAME = 'outbound_filename'
DESTINATION_HEADERS_FILE_PATH = 'headers_destination_file_path'
DESTINATION_BQ_DATASET = 'bq_destination_dataset_name'
SQL_FILE_PATH = 'sql_file_path'
MONITORING_BQ_TABLE_SCHEMA_FILE_PATH = 'schema_file_path'
BQ_TABLE_NAME = 'bq_table_name'
BQ_TO_GCS_FOLDER = 'bq_to_gcs_folder'
MONITORING_BQ_TABLE = 'monitoring_bq_table'
MONITORING_BQ_DATASET = 'monitoring_bq_dataset'
BQ_SOURCE_TABLE = 'bq_source_table'
BQ_SOURCE_DATASET = 'bq_source_dataset'
BQ_TRANSACTION_CONTROL_TABLE = 'bq_transaction_control_table'
BQ_TRANSACTION_CONTROL_DATASET = 'bq_transaction_control_dataset'
DESTINATION_FOLDER = 'destination_folder'
FILENAME_PREFIX = 'filename_prefix'
OUTBOUND_FILE_EXTENSION = 'outbound_file_extension'
DESTINATION_BQ_TABLE_WITH_DATASET = 'destination_bq_table_with_dataset'
FEED_NAME = 'feed.name'
PAUSE_DAG_BY_CONFIG = 'pause_dag_by_config'
SCHEDULING_INTERVAL = 'scheduling_interval'


DAG_DEFAULT_ARGS = {
    "owner": "team-defenders-alerts",
    'capability': 'risk-management',
    'severity': 'P3',
    'sub_capability': 'aml',
    'business_impact': 'AML monitoring and compliance reporting will be impacted, potentially affecting regulatory requirements',
    'customer_impact': 'None',
    "depends_on_past": False,
    "wait_for_downstream": False,
    "max_active_runs": 1,
    "retries": 0,
    "retry_delay": timedelta(seconds=10)
}

############################################################
# Common Methods
############################################################


def read_feed_config(feed_name, sub_folder_name, yaml_config_file):
    aml_config = read_yamlfile_env_suffix(fname=f"{settings.DAGS_FOLDER}/{sub_folder_name}/{yaml_config_file}",
                                          deploy_env=DEPLOY_ENV, env_suffix=DEPLOY_ENV_SUFFIX)
    common_config = aml_config.get("common")
    dag_config = aml_config.get(feed_name)
    outbound_file_extension = read_env_filepattern(dag_config['filename.extension'], DEPLOY_ENV)
    filename_format_with_date = f"{dag_config['filename.prefix']}_{JOB_DATE}" \
                                f".{outbound_file_extension}"
    filename_format_with_datetime = f"{dag_config['filename.prefix']}_{JOB_DATE_TIME}" \
                                    f".{outbound_file_extension}"
    destination_folder = dag_config['outbound.folder']

    variables = {STAGING_BUCKET: dag_config['staging.bucket'],
                 STAGING_FOLDER: f"{dag_config['staging.folder']}/{filename_format_with_date}",
                 OUTBOUND_BUCKET: dag_config['outbound.bucket'],
                 OUTBOUND_FILENAME: f"{destination_folder}/{filename_format_with_datetime}",
                 SOURCE_HEADERS_FILE_PATH: dag_config['headers.source.file_path'],
                 DESTINATION_HEADERS_FILE_PATH: dag_config['headers.destination.file_path'],
                 SQL_FILE_PATH: dag_config['sql.file_path'],
                 MONITORING_BQ_TABLE_SCHEMA_FILE_PATH: f"{settings.DAGS_FOLDER}/{common_config['monitoring.bq_table.schema_file_path']}",
                 BQ_TABLE_NAME: dag_config['bq_table.name'],
                 BQ_TO_GCS_FOLDER: dag_config['bq_to_gcs.folder'],
                 DESTINATION_BQ_DATASET: dag_config['bq_destination_dataset.name'],
                 MONITORING_BQ_DATASET: common_config['monitoring.bq_dataset'],
                 MONITORING_BQ_TABLE: f"{common_config['monitoring.bq_table']}"
                 }

    return variables


def read_dynamic_transaction_feed_config(dag_config):
    outbound_file_extension = read_env_filepattern(dag_config['filename.extension'], DEPLOY_ENV)
    filename_format_with_date = f"{dag_config['filename.prefix']}_{JOB_DATE}" \
                                f".{outbound_file_extension}"
    filename_format_with_datetime = f"{dag_config['filename.prefix']}_{JOB_DATE_TIME}" \
                                    f".{outbound_file_extension}"

    variables = {STAGING_BUCKET: dag_config['staging.bucket'],
                 STAGING_FOLDER: f"{dag_config['staging.folder']}/{dag_config['feed.name']}/{filename_format_with_date}",
                 OUTBOUND_BUCKET: dag_config['outbound.bucket'],
                 OUTBOUND_FILENAME: f"{dag_config['outbound.folder']}/{filename_format_with_datetime}",
                 BQ_TO_GCS_FOLDER: dag_config['bq_to_gcs.folder'] + '/' + dag_config['feed.name'],
                 BQ_TABLE_NAME: dag_config['bq_table.name'],
                 DESTINATION_BQ_DATASET: dag_config['bq_destination_dataset.name'],
                 BQ_SOURCE_TABLE: dag_config['bq_source_table'],
                 BQ_SOURCE_DATASET: dag_config['bq_source_dataset'],
                 BQ_TRANSACTION_CONTROL_TABLE: dag_config['bq_transaction_control_table'],
                 BQ_TRANSACTION_CONTROL_DATASET: dag_config['bq_transaction_control_dataset'],
                 DESTINATION_FOLDER: dag_config['outbound.folder'],
                 FILENAME_PREFIX: dag_config['filename.prefix'],
                 OUTBOUND_FILE_EXTENSION: outbound_file_extension,
                 FEED_NAME: dag_config['feed.name'],
                 SCHEDULING_INTERVAL: dag_config['scheduling_interval'],
                 PAUSE_DAG_BY_CONFIG: dag_config['pause_dag_by_config'],
                 DESTINATION_BQ_TABLE_WITH_DATASET: f"{dag_config['destination_bq_table_dataset']}.{dag_config['bq_table.name']}"
                 }

    return variables


def read_sql_file(sql_file_path):
    sql_file = f'{settings.DAGS_FOLDER}/{sql_file_path}'

    if os.path.exists(sql_file):
        with open(sql_file, 'r') as f:
            content = f.read()
            if DEPLOY_ENV is not None and ENV_PLACEHOLDER in content:
                content = content.replace(ENV_PLACEHOLDER, DEPLOY_ENV)
            return content
    else:
        logger.error(f'{sql_file} does not exist')
        raise (Exception(f'{sql_file} does not exist'))


def run_feed_idl(sql_file_path, bq_destination_dataset_name, bq_feed_table, **context):
    sql = read_sql_file(sql_file_path)

    if context.get("script"):
        job_config = None
    else:
        job_config = bigquery.QueryJobConfig(
            destination=f'{CURATED_PROJECT_ID}.{bq_destination_dataset_name}.{bq_feed_table}',
            write_disposition=WriteDisposition.WRITE_TRUNCATE)

    logger.info(f"job_config: {job_config}\nsql statement: {sql}")
    query_job = run_bq_query(sql, job_config=job_config)

    # Store metrics
    task_metrics = {"bytes_billed": query_job.total_bytes_billed,
                    "count_records": count_records_tbl(f'{CURATED_PROJECT_ID}.{bq_destination_dataset_name}.{bq_feed_table}')}

    context['ti'].xcom_push(key='task_metrics', value=task_metrics)


def run_feed_idl_with_query(query, bq_destination_dataset_name, bq_feed_table, **context):
    if context.get("script"):
        job_config = None
    else:
        job_config = bigquery.QueryJobConfig(
            destination=f'{CURATED_PROJECT_ID}.{bq_destination_dataset_name}.{bq_feed_table}',
            write_disposition=WriteDisposition.WRITE_TRUNCATE)

    logger.info(f"job_config: {job_config}\nsql statement: {query}")
    query_job = run_bq_query(query, job_config=job_config)

    # Store metrics
    task_metrics = {"bytes_billed": query_job.total_bytes_billed,
                    "count_records": count_records_tbl(f'{CURATED_PROJECT_ID}.{bq_destination_dataset_name}.{bq_feed_table}')}

    context['ti'].xcom_push(key='task_metrics', value=task_metrics)


def compose_multiple_files_into_one(staging_bucket, staging_folder, headers_source_file_path,
                                    headers_destination_file_path, bq_to_gcs_folder, **context):
    """
    Takes multiple files and the right column names to combine into one file
    """
    bucket_name = staging_bucket
    header_file_path_source = f"{settings.DAGS_FOLDER}/{headers_source_file_path}"
    header_file_path_destination = f"{headers_destination_file_path}"
    staging_files_location = bq_to_gcs_folder

    # Place the header csv in the source--> destination
    logger.info(f"Placing the file from {header_file_path_source} to gs://{bucket_name}/{header_file_path_destination}")
    upload_blob(bucket_name, header_file_path_source, f"{header_file_path_destination}")

    # *********Compose the file(s) into one file*********

    # Get the list of files
    source_blob_list = list_blobs_with_prefix(bucket_name, staging_files_location, delimiter=None)
    source_blob_list = [file for file in source_blob_list if file != header_file_path_destination]
    # Put the file in the right order
    source_blob_list = [header_file_path_destination] + source_blob_list

    logger.info(f"Composing the following files , {source_blob_list}")

    # Compose
    composed_file = compose_file(bucket_name, source_blob_list, staging_folder)
    file_size = composed_file.size

    # Send the metrics to bq:
    task_metrics = {'source_file_list': str(source_blob_list),
                    'outbound_file_size': file_size}
    context['ti'].xcom_push(key='task_metrics', value=task_metrics)


def compose_infinite_files_into_one(staging_bucket, staging_folder, headers_source_file_path,
                                    headers_destination_file_path, bq_to_gcs_folder, bq_feed_table, **context):
    """
    Takes multiple files and the right column names to combine into one file
    """
    bucket_name = staging_bucket
    header_file_path_source = f"{settings.DAGS_FOLDER}/{headers_source_file_path}"
    header_file_path_destination = f"{headers_destination_file_path}"
    staging_files_location = bq_to_gcs_folder

    chunk_size: int = 32

    # Get all the source files:
    source_blob_list = list_blobs_with_prefix(bucket_name, staging_files_location, delimiter=None)

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
        source_blob_list = list_blobs_with_prefix(bucket_name, staging_files_location, delimiter=None)
        logger.info(f"New files found in the staging folder {staging_files_location} - {source_blob_list}")
        chunk_iteration += 1

    # Compose the remaining files along with the headers file:
    logger.info(f"Placing the file from {header_file_path_source} to gs://{bucket_name}/{header_file_path_destination}")
    upload_blob(bucket_name, header_file_path_source, f"{header_file_path_destination}")

    # Put the file in the right order
    source_blob_list = [f"{header_file_path_destination}"] + source_blob_list
    compose_file(bucket_name, source_blob_list, staging_folder)


def export_bq_table(staging_bucket, bq_to_gcs_folder, bq_destination_dataset, bq_feed_table, export_with_bq_table_header, export_with_compression=False):
    bucket_name = staging_bucket
    destination_folder = f"gs://{bucket_name}/{bq_to_gcs_folder}"
    logger.info(f"Print header for this job is : {export_with_bq_table_header}")
    logger.info(f"destination_folder : {destination_folder}")
    logger.info(f"export table: : {bq_destination_dataset}.{bq_feed_table}")

    # Remove any previous files:
    delete_folder(bucket_name, bq_to_gcs_folder)

    # Export the files to gcs location
    client = bigquery.Client()

    # no headers and pipe delimiter in CSV format
    compression_method = bigquery.Compression.GZIP if export_with_compression else None
    job_config = bigquery.ExtractJobConfig(destination_format=DestinationFormat.CSV, field_delimiter='|',
                                           print_header=export_with_bq_table_header,
                                           compression=compression_method)
    dataset_ref = bigquery.DatasetReference(project=f'{CURATED_PROJECT_ID}', dataset_id=bq_destination_dataset)
    table_ref = bigquery.TableReference(dataset_ref=dataset_ref, table_id=bq_feed_table)
    # specify wildcard uri * in destination_uris to make sure multiple files are generated if larger than 1GB
    extract_job = client.extract_table(source=table_ref,
                                       destination_uris=f'{destination_folder}/{bq_feed_table}-*.csv.gz',
                                       job_config=job_config)
    extract_job.result()
    if extract_job.done() and extract_job.error_result is None:
        logger.info("Extract job completed successfully")


def copy_object(staging_bucket, staging_file, outbound_bucket, outbound_file_name, **context):
    # Copy the file:
    copy_blob(
        staging_bucket, staging_file, outbound_bucket, outbound_file_name
    )

    # Send the metrics to bq:
    task_metrics = {'source': f"{staging_bucket}/{staging_file}",
                    'destination': f"{outbound_bucket}/{outbound_file_name}"}
    context['ti'].xcom_push(key='task_metrics', value=task_metrics)

############################################################
# Monitoring and metrics
############################################################


def metrics_dict_to_struc_str(struct_object_order, metrics_dict=None):
    """This util function is to write the structured object to bq"""
    metrics_dict = {} if metrics_dict is None else metrics_dict
    t = tuple([metrics_dict.get(i) for i in struct_object_order])
    return f"STRUCT{t}".replace("None", "null")


def update_table_schema(table_name, table_dataset, schema_file_path):

    table_id = f"{CURATED_PROJECT_ID}.{table_dataset}.{table_name}"
    client = bigquery.Client()
    table = client.get_table(table_id)

    with open(schema_file_path) as json_file:
        schema = json.load(json_file)

    new_schema = []
    for field in schema:
        fields = []
        if field["type"] == "RECORD" and field["fields"] is not None:
            for sub_field in field["fields"]:
                fields.append(bigquery.SchemaField(sub_field["name"], sub_field["type"], sub_field["mode"]))
        fields = tuple(fields)
        new_schema.append(bigquery.SchemaField(field["name"], field["type"], field["mode"], fields=fields))

    table.schema = new_schema
    client.update_table(table, ["schema"])


def create_metrics_bq_table(monitoring_bq_table, monitoring_bq_dataset, schema_file_path):

    logger.info("Creating the BQ for metrics if it doesn't already exist")
    client = bigquery.Client()
    dataset_ref = bigquery.DatasetReference(CURATED_PROJECT_ID, monitoring_bq_dataset)
    table = bigquery.TableReference(dataset_ref, monitoring_bq_table)
    table = client.create_table(table, exists_ok=True)
    print(f"Created table {CURATED_PROJECT_ID}.{monitoring_bq_dataset}.{monitoring_bq_table}")

    update_table_schema(monitoring_bq_table, monitoring_bq_dataset, schema_file_path)


def collect_metrics(dag_name, monitoring_bq_table, monitoring_bq_dataset, schema_file_path, **context):

    struct_object_order = ["count_records", "bytes_billed", "source_file_list", "source", "destination", "outbound_file_size"]
    monitoring_bq_table_id = f"{CURATED_PROJECT_ID}.{monitoring_bq_dataset}.{monitoring_bq_table}"
    # Create the bq table if not exists
    create_metrics_bq_table(monitoring_bq_table, monitoring_bq_dataset, schema_file_path)

    logger.info("Starting to collect the metrics")
    dr: DagRun = context["dag_run"]  # get the dag_run object
    ti_summary = []
    for task in dr.get_task_instances():
        # Get the custom metrics for the task from xcom
        metrics_dict = context['ti'].xcom_pull(task_ids=task.task_id, key="task_metrics")
        logger.info(f"Received task metrics from xcom for {task.task_id} - {metrics_dict}")
        strutured_bq_metrics_str = metrics_dict_to_struc_str(struct_object_order, metrics_dict)
        logger.info(f"Storing the metrics for {task}")
        task_duration = task.duration if task.duration is not None else "null"
        task_metrics_query = f"""INSERT INTO {monitoring_bq_table_id}
        (DAG_ID, DAG_RUN_ID,  TASK_ID, EXECUTION_TIME, TASK_STATE, TASK_DURATION, metrics_object) VALUES
        ( "{dag_name}" , "{dr.run_id}"   ,"{task.task_id}", "{task.execution_date}", "{task.state}", {task_duration}, {strutured_bq_metrics_str}) """
        logger.info(
            f"Inserting the metrics for {task.task_id} into table {monitoring_bq_table_id} "
            f"using the query {task_metrics_query}")
        run_bq_query(task_metrics_query)
        ti_summary.append(f'Task: {task.task_id}, State: {task.state}, ExecutionDate: {task.execution_date}\n')
    logger.info(ti_summary)
    logger.info(f"Dag run object: {dr}")
    return ti_summary


def final_task(**context):
    for task_instance in context['dag_run'].get_task_instances():
        if task_instance.state not in (State.SUCCESS, State.SKIPPED) and task_instance.task_id != context['task_instance'].task_id:
            raise AirflowFailException("Task {} failed. Failing this DAG run".format(task_instance.task_id))
