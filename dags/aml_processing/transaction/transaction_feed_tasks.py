import logging
import pendulum
import json

from datetime import datetime, date
from typing import Final
from google.cloud import bigquery
from airflow.utils.state import State
from airflow.exceptions import AirflowFailException, AirflowConfigException
from aml_processing import feed_commons as commons
from util.bq_utils import run_bq_query
from util.miscutils import read_variable_or_file, read_env_filepattern
from airflow.models.dagrun import DagRun
from aml_processing.transaction.agg_domain_at31 import AGG_BQTABLE_AT31
from aml_processing.transaction.transaction import process_transaction_feed
from util.bq_utils import check_bq_table_exists, create_or_replace_table
from util.gcs_utils import copy_blob
from airflow import settings


############################################################
# Configs and constants
############################################################


# Config files
GCP_CONFIG = read_variable_or_file('gcp_config')
DEPLOY_ENV = GCP_CONFIG['deployment_environment_name']

# GCS Bucket / Directory (with dates)
TORONTO_TZ = pendulum.timezone('America/Toronto')
CURRENT_DATETIME = datetime.now(tz=TORONTO_TZ)
JOB_DATE = f"{CURRENT_DATETIME.strftime('%Y%m%d')}"
JOB_DATE_TIME = f"{CURRENT_DATETIME.strftime('%Y%m%d%H%M%S')}"

# Index names
STAGING_BUCKET = 'staging_bucket'
STAGING_FOLDER = 'staging_folder'
OUTBOUND_BUCKET = 'outbound_bucket'
OUTBOUND_FILENAME = 'outbound_filename'
DESTINATION_BQ_DATASET = 'bq_destination_dataset_name'
BQ_TABLE_NAME = 'bq_table_name'
BQ_TO_GCS_FOLDER = 'bq_to_gcs_folder'
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
SCHEDULEING_INTERVAL = 'scheduling_interval'
SUCCESS_STATUS = "success"
RUNNING_STATUS = "running"

# Tables
TRANSACTION_FEED_CTL_TABLE = f"pcb-{DEPLOY_ENV}-processing.domain_aml.TRANSACTION_FEED_CONTROL_TABLE"
TRANSACTION_FEED_HISTORICAL_RECORDS_TABLE = f"pcb-{DEPLOY_ENV}-curated.cots_aml_verafin.TRANSACTION_FEED_HISTORICAL_RECORDS_TABLE"

# Schema_file_path
TBL_CURATED_TRANSACTION_FEED_SCHEMA = f"{settings.DAGS_FOLDER}/aml_processing/transaction/TRANSACTION_FEED_SCHEMA.json"

# Constants
IDL: Final[str] = "IDL"
DAILY: Final[str] = "DAILY"
MANUAL: Final[str] = "MANUAL"

logger = logging.getLogger(__name__)

############################################################
# Common Methods
############################################################


def read_dynamic_transaction_feed_config(dag_config):
    outbound_file_extension = read_env_filepattern(
        dag_config['filename.extension'], DEPLOY_ENV)
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
                 SCHEDULEING_INTERVAL: dag_config['scheduling_interval'],
                 PAUSE_DAG_BY_CONFIG: dag_config['pause_dag_by_config'],
                 DESTINATION_BQ_TABLE_WITH_DATASET: f"{dag_config['destination_bq_table_dataset']}.{dag_config['bq_table.name']}"
                 }

    return variables


# Method to fetch the LAST_RECORD_DATE for the most recent successful DAG Run for the given feed
def get_last_record_date(rail_type):
    sql = f"""
            SELECT
                MAX(LAST_RECORD_DATE) AS LAST_RECORD_DATE
            FROM
                `{TRANSACTION_FEED_CTL_TABLE}`
            WHERE
                LOWER(RAIL_TYPE) = LOWER('{rail_type}')
                AND LOWER(STATUS) = LOWER('{SUCCESS_STATUS}')
                AND LOWER(DAG_RUN_TYPE) = 'scheduled'
                AND RECORDS_COUNT > 0
    """
    logger.info(f'Fetching last record date from {TRANSACTION_FEED_CTL_TABLE} using query: {sql}')
    result = run_bq_query(sql).result()
    if result.total_rows > 0:
        last_record_date = next(result).get("LAST_RECORD_DATE")
        logger.info(f"Last Record Date: {last_record_date}")
        return last_record_date
    logger.info(f"No successful runs found for {rail_type}. Assuming first run.")
    return None


# Method to fetch the start_time and end time to create the extraction for the given feed
def get_transaction_time_range_and_count(rail_type, start_time=None, end_time=None, is_idl=False, last_record_date=None):
    where_clause = f"RAIL_TYPE = '{rail_type}'"
    if is_idl:
        where_clause += " AND REC_LOAD_TIMESTAMP BETWEEN DATETIME_SUB(CURRENT_DATETIME('America/Toronto'), INTERVAL 13 MONTH) AND CURRENT_DATETIME('America/Toronto')"
    elif last_record_date:
        where_clause += f" AND REC_LOAD_TIMESTAMP > '{last_record_date}'"
    elif start_time and end_time:
        where_clause += f" AND REC_LOAD_TIMESTAMP BETWEEN '{start_time}' AND '{end_time}'"

    sql = f"""
            SELECT
                MIN(REC_LOAD_TIMESTAMP) AS START_TIME,
                MAX(REC_LOAD_TIMESTAMP) AS END_TIME,
                COUNT(*) AS RECORDS_COUNT
            FROM
                `{AGG_BQTABLE_AT31}`
            WHERE
                {where_clause}
        """
    logger.info(f'Fetching start_time, end_time, and records count from {AGG_BQTABLE_AT31} using query: {sql}')
    result = run_bq_query(sql).result()
    row = next(result, None)
    if row:
        return {
            'start_time': row.get("START_TIME"),
            'end_time': row.get("END_TIME"),
            'records_count': int(row.get("RECORDS_COUNT", 0))
        }
    return {'start_time': None, 'end_time': None, 'records_count': 0}


def create_curated_transaction_feed_historical_records_table(historical_records_table_schema, **context):
    if not check_bq_table_exists(TRANSACTION_FEED_HISTORICAL_RECORDS_TABLE):
        partition_column = "FILE_SENT_DATE"
        partition_type = bigquery.TimePartitioningType.MONTH
        create_or_replace_table(TRANSACTION_FEED_HISTORICAL_RECORDS_TABLE, historical_records_table_schema, partitioning_column=partition_column, partition_type=partition_type)


def process_transaction_feed_task(feed_name, dst_table, **context):
    dr = context['dag_run']
    ti = context['task_instance']
    rail_type = context['params'].get('feed', feed_name)

    # Ensure control table exists
    if not check_bq_table_exists(TRANSACTION_FEED_CTL_TABLE):
        logger.info(f"Control table {TRANSACTION_FEED_CTL_TABLE} does not exist. Creating for IDL run.")
        create_control_bq_table(TRANSACTION_FEED_CTL_TABLE)

    # Determine run type and dates
    if dr.run_type == 'manual':
        start_time = parse_date(context['params'].get('start_time'))
        end_time = parse_date(context['params'].get('end_time'))
        if not start_time or not end_time or start_time > end_time:
            raise AirflowFailException("Manual trigger requires valid start_time and end_time.")
        run_type = MANUAL
        result = get_transaction_time_range_and_count(rail_type, start_time, end_time)
    else:
        last_record_date = get_last_record_date(rail_type)
        if last_record_date is None:
            # First run (IDL): 13-month lookback
            run_type = IDL
            result = get_transaction_time_range_and_count(rail_type, is_idl=True)
        else:
            # Daily run: since last successful run
            run_type = DAILY
            result = get_transaction_time_range_and_count(rail_type, last_record_date=last_record_date)

    start_time = result['start_time']
    end_time = result['end_time']
    records_count = result['records_count']
    logger.info(f"Run type: {run_type}, Start time: {start_time}, End time: {end_time}, Records count: {records_count}")

    # Log run in control table
    values = {
        "RUN_ID": dr.run_id,
        "RAIL_TYPE": rail_type,
        "TYPE": run_type,
        "STATUS": RUNNING_STATUS,
        "DAG_RUN_TYPE": dr.run_type,
        "FIRST_RECORD_DATE": start_time,
        "LAST_RECORD_DATE": end_time,
        "RECORDS_COUNT": records_count
    }
    logger.info(f"Logging values in the control table : {values}")
    insert_into_transaction_feed_control_table(TRANSACTION_FEED_CTL_TABLE, values)
    ti.xcom_push(key='records_count', value=records_count)

    # Process records if any
    if records_count > 0 and (start_time not in [None, 'NULL']) and (end_time not in [None, 'NULL']):
        job_query = process_transaction_feed(rail_type, start_time, end_time, dst_table)
        logger.info(f"Processing {rail_type} feed using query: {job_query}")
        run_bq_query(job_query)


def control_table_update(feed_name, **context):
    rail_type = context['params'].get('feed', feed_name)
    dr: DagRun = context["dag_run"]

    # Depends on task id  to figureout whether batch or delta was ran as part of the pipeline
    task_metrics_dict = context['ti'].xcom_pull(task_ids="copy_object_to_outbound_bucket_task", key="task_metrics")
    filename = "NULL"
    if task_metrics_dict is not None:
        filename = task_metrics_dict.get('destination', 'NULL')

    upstream_task_failure = False
    for task_instance in context['dag_run'].get_task_instances():
        logger.info(f" Task ID : {task_instance.task_id} , Task State : {task_instance.state}")
        if task_instance.state not in (State.SUCCESS, State.SKIPPED, State.NONE) and task_instance.task_id != context['task_instance'].task_id:
            upstream_task_failure = True
            break

    logger.info(f'upstream_task_failure : {upstream_task_failure}')

    if (not upstream_task_failure):
        task_status = 'success'
    if upstream_task_failure:
        task_status = 'failed'

    # Calling the update_control_table() to set the status and filenames
    values = {
        "FILE_NAME": filename,
        "STATUS": task_status
    }
    update_control_table(TRANSACTION_FEED_CTL_TABLE, dr.run_id, rail_type, values)
    return 'ok'


def create_control_bq_table(control_bq_table):
    logger.info("Creating the BQ control table if it doesn't already exists")
    create_table_query = f"""
                            CREATE TABLE IF NOT EXISTS {control_bq_table}
                            (
                                RUN_ID              STRING,
                                RAIL_TYPE           STRING,
                                TYPE                STRING,
                                RUN_DATE            DATETIME,
                                STATUS              STRING,
                                FIRST_RECORD_DATE   DATETIME,
                                LAST_RECORD_DATE    DATETIME,
                                FILE_NAME           STRING,
                                DAG_RUN_TYPE        STRING,
                                RECORDS_COUNT       INT64
                            )
                        """
    run_bq_query(create_table_query)


def update_control_table(control_bq_table, dag_run_id, rail_type, values):
    update_query = f"""
                        UPDATE `{control_bq_table}`
                            SET
                                STATUS = "{values['STATUS']}",
                                FILE_NAME = "{values['FILE_NAME']}"
                        WHERE
                            LOWER(RUN_ID) = LOWER('{dag_run_id}')
                            AND LOWER(RAIL_TYPE) = LOWER('{rail_type}')
                    """
    logger.info(f'Updating the {control_bq_table} using query : {update_query}')
    run_bq_query(update_query)


def insert_into_transaction_feed_control_table(control_bq_table, values):
    insert_query = f"""
                        INSERT INTO `{control_bq_table}`
                        (
                            RUN_ID,
                            RAIL_TYPE,
                            TYPE,
                            RUN_DATE,
                            STATUS,
                            DAG_RUN_TYPE,
                            FIRST_RECORD_DATE,
                            LAST_RECORD_DATE,
                            RECORDS_COUNT
                        )
                        VALUES
                        (
                            "{values['RUN_ID']}",
                            "{values['RAIL_TYPE']}",
                            "{values['TYPE']}",
                            CURRENT_DATETIME('America/Toronto'),
                            "{values['STATUS']}",
                            "{values['DAG_RUN_TYPE']}",
                            SAFE_CAST("{values['FIRST_RECORD_DATE']}" AS DATETIME),
                            SAFE_CAST("{values['LAST_RECORD_DATE']}" AS DATETIME),
                            SAFE_CAST("{values['RECORDS_COUNT']}" AS INT64)
                        )
    """
    logger.info(f'Inserting into {control_bq_table} using query : {insert_query}')
    run_bq_query(insert_query)


def insert_into_curated_transaction_feed_historical_records_table(feed_name, transaction_feed_table, **context):
    dr: DagRun = context['dag_run']
    ti = context['task_instance']

    rail_type = context['params'].get('feed', feed_name)
    run_id = dr.run_id

    task_metrics_dict = ti.xcom_pull(task_ids='copy_object_to_outbound_bucket_task', key='task_metrics')
    filename = None
    if task_metrics_dict is not None:
        filename = task_metrics_dict.get('destination')
    updated_filename = f"'{filename}'" if filename is not None else 'NULL'

    with open(TBL_CURATED_TRANSACTION_FEED_SCHEMA) as transaction_feed_schema_file:
        transaction_feed_schema = json.load(transaction_feed_schema_file)
        column_list_transaction_feed = ',\n'.join([dict.get('name') for dict in transaction_feed_schema])
        column_list_historical_data = ',\n'.join([f"SAFE_CAST({dict.get('name')} AS STRING) AS {dict.get('name')}" for dict in transaction_feed_schema])

    insert_query = f"""
                        INSERT INTO `{TRANSACTION_FEED_HISTORICAL_RECORDS_TABLE}`
                        (
                            {column_list_transaction_feed},
                            RUN_ID,
                            RAIL_TYPE,
                            FILENAME,
                            FILE_SENT_DATE
                        )
                        SELECT
                            {column_list_historical_data},
                            '{run_id}'                                  AS RUN_ID,
                            '{rail_type}'                               AS RAIL_TYPE,
                            {updated_filename}                          AS FILENAME,
                            CURRENT_DATE('America/Toronto')             AS FILE_SENT_DATE
                        FROM
                            `{transaction_feed_table}`
    """
    logger.info(f'Inserting into {TRANSACTION_FEED_HISTORICAL_RECORDS_TABLE} using query : {insert_query}')
    run_bq_query(insert_query)


def parse_date(date_txt: str) -> date:
    if date_txt is None or date_txt.strip() == '':
        return None

    try:
        parsed_date = datetime.strptime(date_txt, '%Y-%m-%d %H:%M:%S')
        return parsed_date
    except ValueError:
        msg = f"Invalid date value:{date_txt}, expected format: yyyy-MM-dd HH:mm:ss, e.g. 2022-10-19 12:00:00"
        logger.error(msg)
        raise AirflowConfigException(msg)
