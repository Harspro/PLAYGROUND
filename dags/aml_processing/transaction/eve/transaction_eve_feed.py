
import json
from airflow.models.dagrun import DagRun
from util.miscutils import (
    save_job_to_control_table
)

import pytz
import logging
from datetime import datetime, timedelta

from airflow import settings
from airflow.exceptions import AirflowFailException

from aml_processing import feed_commons as commons
from util.bq_utils import run_bq_query
from util.miscutils import get_dagrun_last
from aml_processing.transaction import transaction_feed_tasks as tasks

logger = logging.getLogger(__name__)
current_datetime = datetime.now(pytz.timezone('America/Toronto'))


def build_control_record_saving_job(dag_name, **context):
    """
    Args:
        dag_name: Name of the DAG.
        context: Airflow task context.
    """
    ti = context['task_instance']
    dr: DagRun = context["dag_run"]
    records_count = int(ti.xcom_pull(task_ids='check_control_table_updates_task', key='records_count') or 0)
    task_metrics_dict = ti.xcom_pull(task_ids="copy_object_to_outbound_bucket_task", key="task_metrics")
    filename = "NULL"
    if task_metrics_dict is not None:
        filename = task_metrics_dict.get('destination', 'NULL')

    # Save to control table
    start_time = ti.xcom_pull(task_ids='check_control_table_updates_task', key='start_time')
    end_time = ti.xcom_pull(task_ids='check_control_table_updates_task', key='end_time')

    run_type = dr.run_type

    job_params = {
        'start_time': start_time,
        'end_time': end_time,
        'records_count': records_count,
        'filename': filename,
        'run_type': run_type
    }

    logger.info(f"Saving control table entry for {dag_name}")
    save_job_to_control_table(job_params, end_time, **context)


def check_control_table_updates(job_config, **context):
    """
    Check control table for last run and determine job start/end dates. Push records_count to XCom.
    Skips downstream tasks if no records are found.

    Args:
        job_config: Configuration dictionary for the job.
        context: Airflow task context.
    Returns:
        End date for the job.
    """
    dr: DagRun = context["dag_run"]
    ti = context['task_instance']
    dag_name = job_config['dag_name']
    # Get start/end dates from params (manual trigger) or control table
    start_time = tasks.parse_date(context['params'].get('start_time'))
    end_time = tasks.parse_date(context['params'].get('end_time'))

    if dr.run_type == 'manual':
        if (not start_time or not end_time) or (start_time > end_time):
            raise AirflowFailException("Manual trigger requires both start_time and end_time.")
        logger.info(f"Manual run: start_time={start_time}, end_time={end_time}")
    else:
        # Check control table for last run
        filter = "AND STARTS_WITH(LOWER(dag_run_id), 'scheduled')"
        result = get_dagrun_last(dag_name, filter)
        if result.total_rows == 0:
            start_time = job_config['idl_cutoff_date']
            logger.info(f"First run of {dag_name}. Using IDL cutoff: {start_time}")
        else:
            last_run = result.to_dataframe()['load_timestamp'].iloc[0]
            last_run_tms = last_run.strftime('%Y-%m-%d %H:%M:%S')
            logger.info(f"Last run timestamp: {last_run_tms}")
            start_time = (current_datetime - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')

        end_time = current_datetime.strftime('%Y-%m-%d %H:%M:%S')

    if not start_time or not end_time:
        raise AirflowFailException("Invalid start_time or end_time.")

    # Check for records in the source table
    bq_data_check_query = commons.read_sql_file(job_config['sql_bq_data_check_query_path']).format(
        BQTABLE_PRODUCT_TRANSACTION=job_config['bqtable_product_transaction'],
        TRANSACTION_HISTORY_TABLE=job_config['transaction_history_table'],
        BQTABLE_CUSTOMER=job_config['bqtable_customer'],
        start_time=start_time,
        end_time=end_time
    )
    logger.info(f"Checking number of records in {job_config['bqtable_product_transaction']} to load using query : {bq_data_check_query}")
    bq_data_check_results = run_bq_query(bq_data_check_query).result()
    records_count = int(next(bq_data_check_results).get('RECORDS_COUNT') or 0)
    logger.info(f"Found {records_count} records in PRODUCT_TRANSACTION")

    # Push XComs
    ti.xcom_push(key='start_time', value=str(start_time))
    ti.xcom_push(key='end_time', value=str(end_time))
    ti.xcom_push(key='records_count', value=records_count)

    return end_time


def sql_create_eve_transaction_feed_table(job_config, curated_transaction_feed_historical_records_table_schema, **context) -> str:
    tasks.create_curated_transaction_feed_historical_records_table(curated_transaction_feed_historical_records_table_schema)
    sql = commons.read_sql_file(job_config['sql_eve_create_table_path']).format(
        BQTABLE_EVE_TRANSACTION_FEED=job_config['bqtable_eve_transaction_feed'])
    return sql


def sql_insert_into_eve_table(job_config, start, end, **context) -> str:
    sql = commons.read_sql_file(job_config['sql_eve_insert_table_path']).format(
        BQTABLE_EVE_TRANSACTION_FEED=job_config['bqtable_eve_transaction_feed'],
        BQTABLE_PRODUCT_TRANSACTION=job_config['bqtable_product_transaction'],
        TRANSACTION_HISTORY_TABLE=job_config['transaction_history_table'],
        BQTABLE_CUSTOMER=job_config['bqtable_customer'],
        start_time=start,
        end_time=end)
    return sql


def insert_into_curated_transaction_feed_historical_records_table(feed_name, job_config, **context):
    ti = context['task_instance']
    dr: DagRun = context['dag_run']
    rail_type = feed_name
    run_id = dr.run_id
    tbl_curated_transaction_feed_schema = f"{settings.DAGS_FOLDER}/{job_config['tbl_curated_transaction_feed_schema']}"

    task_metrics_dict = ti.xcom_pull(task_ids='copy_object_to_outbound_bucket_task', key='task_metrics')
    filename = None
    if task_metrics_dict is not None:
        filename = task_metrics_dict.get('destination')
    updated_filename = f"'{filename}'" if filename is not None else 'NULL'

    with open(tbl_curated_transaction_feed_schema) as transaction_feed_schema_file:
        transaction_feed_schema = json.load(transaction_feed_schema_file)
        column_list_transaction_feed = ',\n'.join([dict.get('name') for dict in transaction_feed_schema])
        column_list_historical_data = ',\n'.join([f"SAFE_CAST({dict.get('name')} AS STRING) AS {dict.get('name')}" for dict in transaction_feed_schema])

    insert_query = commons.read_sql_file(job_config['sql_feed_history_table_path']).format(
        TRANSACTION_HISTORY_TABLE=job_config['transaction_history_table'],
        column_list_transaction_feed=column_list_transaction_feed,
        column_list_historical_data=column_list_historical_data,
        run_id=run_id,
        rail_type=rail_type,
        updated_filename=updated_filename,
        transaction_feed_table=job_config['bqtable_eve_transaction_feed'])
    logger.info(f"Inserting records in {job_config['transaction_history_table']} from {job_config['bqtable_eve_transaction_feed']} using query : {insert_query}")
    run_bq_query(insert_query)
