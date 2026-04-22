import pendulum
import json
import logging
from typing import Final
from datetime import datetime

from google.cloud import bigquery
from airflow import settings

import util.constants as consts
from util.miscutils import read_variable_or_file, read_file_env
from util.bq_utils import check_bq_table_exists, create_or_replace_table

GCP_CONFIG = read_variable_or_file('gcp_config')
DEPLOY_ENV = GCP_CONFIG['deployment_environment_name']

PAYMENTS_AUDIT_CONTROL_TABLE = f"pcb-{DEPLOY_ENV}-curated.domain_payments.PAYMENTS_AUDIT_CONTROL"
PAYMENTS_AUDIT_CONTROL_TABLE_SCHEMA = f"{settings.DAGS_FOLDER}/payments_processing/PAYMENTS_AUDIT_CONTROL_TABLE_SCHEMA.json"
local_tz = datetime.now(pendulum.timezone('America/Toronto'))
CURRENT_DATETIME = local_tz.strftime('%Y-%m-%dT%H:%M:%S.%f')
sql_file_path = f'{settings.DAGS_FOLDER}/payments_processing/sql/'
CREATE_DATE_COLUMN: Final = 'create_date_column'
AMOUNT_COLUMN: Final = 'amount_column'
FILE_CREATE_DT_PLACEHOLDER: Final = '{file_create_dt}'


def start_audit_task(inbound_file_name: str, **context):
    bigquery_client = bigquery.Client()
    dag_id = context['dag'].dag_id
    dag_run_id = context['run_id']
    payments_audit_table_id = f"{PAYMENTS_AUDIT_CONTROL_TABLE}"
    if not check_bq_table_exists(PAYMENTS_AUDIT_CONTROL_TABLE):
        create_or_replace_table(PAYMENTS_AUDIT_CONTROL_TABLE, PAYMENTS_AUDIT_CONTROL_TABLE_SCHEMA)
    logging.info(f"Starting audit control for {dag_id} and {dag_run_id}")

    start_audit_control = f"""INSERT INTO {payments_audit_table_id}
        (DAG_RUN_ID, DAG_ID, INBOUND_FILE_NAME, JOB_START_TIME, STATUS) VALUES
        ( "{dag_run_id}", "{dag_id}", "{inbound_file_name}", '{CURRENT_DATETIME}', "STARTED") """

    logging.info(
        f"Inserting the job run details for {dag_id} and {dag_run_id} into table {payments_audit_table_id} "
        f"using the query {start_audit_control}")

    insert_stats = bigquery_client.query(start_audit_control)
    insert_stats.result()

    context['ti'].xcom_push(key='job_start_time', value=CURRENT_DATETIME)


def collect_job_stats(bigquery_config: dict, file_name: str, trigger_dag_id: str, **context):
    bigquery_client = bigquery.Client()
    job_start_time = context['ti'].xcom_pull(task_ids='preprocessing.start_audit_task', key='job_start_time')
    logging.info(f"Inbound DAG was started at: {job_start_time}")

    record_count_column = bigquery_config.get(consts.TABLES).get('DETL').get(consts.RECORD_COUNT_COLUMN)
    amount_column = bigquery_config.get(consts.TABLES).get('TRLR').get(AMOUNT_COLUMN)
    bq_processing_project_name = GCP_CONFIG[consts.PROCESSING_ZONE_PROJECT_ID]
    bq_dataset_name = bigquery_config.get(consts.DATASET_ID)
    tsys_count_str = context['ti'].xcom_pull(task_ids=f"{consts.TRAILER_SEGMENT_NAME}.rec_count_validate",
                                             key=f"{consts.RECORD_COUNT}")
    tsys_count_json = json.loads(tsys_count_str)
    tsys_count = tsys_count_json.get(record_count_column).get('0')
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    payments_audit_table_id = f"{PAYMENTS_AUDIT_CONTROL_TABLE}"
    trlr_table = bigquery_config.get(consts.TABLES).get('TRLR').get(consts.TABLE_NAME)

    file_create_dt = context['ti'].xcom_pull(task_ids='postprocessing.get_file_create_dt_task', key='file_create_dt')
    logging.info(f"file_create_dt for this execution of {dag_id} and {run_id} is {file_create_dt}")

    total_amount_sql = f"SELECT {amount_column} FROM {bq_processing_project_name}.{bq_dataset_name}.{trlr_table}_{consts.EXTERNAL_TABLE_SUFFIX}"
    logging.info(f"extracting file create date using the query {total_amount_sql}")
    total_amount_results = bigquery_client.query(total_amount_sql).result().to_dataframe()
    total_amount = total_amount_results[amount_column].values[0]
    logging.info(f"total amount processed in this execution of {dag_id} and {run_id} is {total_amount}")
    outbound_stats_sql = f"""SELECT
                               OUTBOUND_DAG_ID,
                               OUTBOUND_RUN_ID,
                               OUTBOUND_FILE_NAME,
                               OUTBOUND_RECORD_COUNT,
                               OUTBOUND_TOTAL_AMOUNT
                             FROM
                               {payments_audit_table_id}
                             WHERE
                               FILE_CREATE_DT='{file_create_dt}'
                               AND STATUS='OUTBOUND SUCCESS'
                               AND DAG_ID='{dag_id}'"""
    outbound_stats_results = bigquery_client.query(outbound_stats_sql).result().to_dataframe()
    if not outbound_stats_results.empty:
        outbound_dag_run_id = outbound_stats_results['OUTBOUND_RUN_ID'].values[0]
        outbound_file_name = outbound_stats_results['OUTBOUND_FILE_NAME'].values[0]
        outbound_record_count = outbound_stats_results['OUTBOUND_RECORD_COUNT'].values[0]
        outbound_total_amount = outbound_stats_results['OUTBOUND_TOTAL_AMOUNT'].values[0]
    else:
        logging.info(f"No outbound dag executed for {dag_id} and {run_id}, so default values added")
        outbound_dag_run_id = 'None'
        outbound_file_name = 'None'
        outbound_record_count = 0
        outbound_total_amount = 0
    logging.info(f"Outbound dag values are: {trigger_dag_id}, {outbound_dag_run_id}, {outbound_file_name} and {outbound_record_count}")
    logging.info(f"Storing the metrics for {dag_id} and {run_id}")

    insert_audit_control = f"""INSERT INTO {payments_audit_table_id}
        (DAG_RUN_ID, DAG_ID, FILE_CREATE_DT, INBOUND_FILE_NAME, INBOUND_RECORD_COUNT, INBOUND_TOTAL_AMOUNT,
         OUTBOUND_DAG_ID, OUTBOUND_RUN_ID, OUTBOUND_FILE_NAME, OUTBOUND_RECORD_COUNT, OUTBOUND_TOTAL_AMOUNT,
         JOB_START_TIME, JOB_END_TIME, STATUS) VALUES
        ( "{run_id}", "{dag_id}", '{file_create_dt}', "{file_name}",{tsys_count}, {total_amount},
         "{trigger_dag_id}","{outbound_dag_run_id}", "{outbound_file_name}", {outbound_record_count}, {outbound_total_amount}, '{job_start_time}', '{CURRENT_DATETIME}', "SUCCESS") """
    logging.info(
        f"updating the stats for {dag_id} and {run_id} into table {payments_audit_table_id} "
        f"using the query {insert_audit_control}")
    insert_stats = bigquery_client.query(insert_audit_control)
    insert_stats.result()


def extract_outbound_job_stats(outbound_file_name: str, file_create_dt: str, inbound_dag_run_id: str, dag_config: dict, **context):
    client = bigquery.Client()
    outbound_dag_id = context['dag'].dag_id
    outbound_dag_run_id = context['run_id']
    inbound_dag_id = dag_config.get('inbound_dag_id')
    payments_audit_table_id = f"{PAYMENTS_AUDIT_CONTROL_TABLE}"
    sql_file = f"{outbound_dag_id}_audit.sql"
    audit_table_sql = read_file_env(sql_file_path + sql_file, DEPLOY_ENV)
    logging.info(f"extracting stats for {outbound_dag_id} using: {audit_table_sql}")
    extract_stats_sql = client.query(audit_table_sql.replace(FILE_CREATE_DT_PLACEHOLDER, file_create_dt))
    extract_stats_results = extract_stats_sql.result().to_dataframe()
    outbound_record_count = extract_stats_results['OUTBOUND_RECORD_COUNT'].values[0]
    outbound_total_amount = extract_stats_results['OUTBOUND_TOTAL_AMOUNT'].values[0]
    logging.info(f"total amount and total records processed for {outbound_dag_id} are {outbound_total_amount} and {outbound_record_count}")

    logging.info(f"Storing the metrics for {outbound_dag_id} and {outbound_dag_run_id}")

    insert_audit_control = f"""INSERT INTO {payments_audit_table_id}
            (DAG_RUN_ID, DAG_ID, FILE_CREATE_DT, INBOUND_FILE_NAME, INBOUND_RECORD_COUNT, INBOUND_TOTAL_AMOUNT,
             OUTBOUND_DAG_ID, OUTBOUND_RUN_ID, OUTBOUND_FILE_NAME, OUTBOUND_RECORD_COUNT, OUTBOUND_TOTAL_AMOUNT,
             JOB_START_TIME, JOB_END_TIME, STATUS) VALUES
            ( "{inbound_dag_run_id}", "{inbound_dag_id}", '{file_create_dt}', null, null, null,
             "{outbound_dag_id}","{outbound_dag_run_id}", "{outbound_file_name}", {outbound_record_count},
             {outbound_total_amount}, '{CURRENT_DATETIME}', '{CURRENT_DATETIME}', "OUTBOUND SUCCESS") """
    logging.info(
        f"updating the stats for {outbound_dag_id} and {outbound_dag_run_id} into table {payments_audit_table_id} "
        f"using the query {insert_audit_control}")
    insert_stats = client.query(insert_audit_control)
    insert_stats.result()
    logging.info(f"Outbound stats are stored to the audit table for: {outbound_dag_id} and {outbound_dag_run_id}")
