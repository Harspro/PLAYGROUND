import io
import json
import logging

from google.cloud import bigquery
from datetime import datetime
import uuid
import util.constants as consts

from util.miscutils import read_variable_or_file

logger = logging.getLogger(__name__)

# -----------Example usage ---------------------
# def create_entry(**kwargs):
#     job_type = "ETL"
#     job_name = "daily_data_load"
#     source_name = "source_system"
#     target_name = "target_system"
#     create_user_id = "airflow_user"
#     create_function_name = "data_load_dag"
#     batch_run_log_uid = create_execution_entry(
#         job_type=job_type,
#         job_name=job_name,
#         source_name=source_name,
#         target_name=target_name,
#         create_user_id=create_user_id,
#         create_function_name=create_function_name
#     )
#     # Push the UID to XCom for use in the update task
#     kwargs['ti'].xcom_push(key='batch_run_log_uid', value=batch_run_log_uid)
#
#
# def update_entry(**kwargs):
#     ti = kwargs['ti']
#     batch_run_log_uid = ti.xcom_pull(key='batch_run_log_uid', task_ids='create_entry_task')
#     update_execution_entry(
#         batch_run_log_uid=batch_run_log_uid,
#         run_end=datetime.now(),
#         read_cnt=1000,
#         write_cnt=950,
#         status="COMPLETED",
#         update_user_id="airflow_user",
#         update_function_name="data_load_dag"
#     )
#
#
# create_entry_task = PythonOperator(
#     task_id='create_entry_task',
#     python_callable=create_entry,
#     provide_context=True,
# )
#
# update_entry_task = PythonOperator(
#     task_id='update_entry_task',
#     python_callable=update_entry,
#     provide_context=True,
# )

gcp_config = read_variable_or_file('gcp_config')
deploy_env = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]


def get_bigquery_client():
    """Returns a BigQuery client."""
    return bigquery.Client()


def create_execution_entry(job_type, job_name, source_name, target_name, create_user_id, create_function_name):
    """Creates an initial entry in the BATCH_RUN_LOG table and returns the BATCH_RUN_LOG_UID."""
    client = get_bigquery_client()

    # Generate a unique identifier for the batch run log entry
    batch_run_log_uid = str(uuid.uuid4())  # Using the first 64 bits of UUID4 for uniqueness

    # Current datetime
    current_datetime = datetime.now().isoformat()

    # Define the row to insert
    log_entry = {
        "BATCH_RUN_LOG_UID": batch_run_log_uid,
        "JOB_TYPE": job_type,
        "JOB_NAME": job_name,
        "RUN_START": current_datetime,
        "SOURCE_NAME": source_name,
        "TARGET_NAME": target_name,
        "READ_CNT": 0,
        "WRITE_CNT": 0,
        "NONDATA_CNT": 0,
        "FILTER_CNT": 0,
        "STATUS": "RUNNING",
        "CREATE_DT": current_datetime,
        "CREATE_USER_ID": create_user_id,
        "CREATE_FUNCTION_NAME": create_function_name,
        "UPDATE_DT": current_datetime,
        "UPDATE_USER_ID": create_user_id,
        "UPDATE_FUNCTION_NAME": create_function_name
    }
    data_as_file = io.BytesIO(bytes(json.dumps(log_entry), 'utf-8'))
    # Insert the row into the table
    table_id = f"pcb-{deploy_env}-landing.domain_payments_ops.BATCH_RUN_LOG"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = client.load_table_from_file(data_as_file, table_id, job_config=job_config)

    try:
        load_job.result()
    except Exception:
        logger.error(load_job.errors)
        raise
    return batch_run_log_uid


def update_execution_entry(batch_run_log_uid, run_end=None, read_cnt=None, write_cnt=None, nondata_cnt=None,
                           filter_cnt=None, status=None, update_user_id=None, update_function_name=None):
    """Updates an existing entry in the BATCH_RUN_LOG table."""
    client = get_bigquery_client()

    # Current datetime
    current_datetime = datetime.now()

    # Build the update statement
    update_fields = []
    if run_end:
        update_fields.append(f"RUN_END = '{run_end}'")
    if read_cnt is not None:
        update_fields.append(f"READ_CNT = {read_cnt}")
    if write_cnt is not None:
        update_fields.append(f"WRITE_CNT = {write_cnt}")
    if nondata_cnt is not None:
        update_fields.append(f"NONDATA_CNT = {nondata_cnt}")
    if filter_cnt is not None:
        update_fields.append(f"FILTER_CNT = {filter_cnt}")
    if status:
        update_fields.append(f"STATUS = '{status}'")
    if update_user_id:
        update_fields.append(f"UPDATE_USER_ID = '{update_user_id}'")
    if update_function_name:
        update_fields.append(f"UPDATE_FUNCTION_NAME = '{update_function_name}'")
    update_fields.append(f"UPDATE_DT = '{current_datetime}'")

    update_statement = f"""
    UPDATE `pcb-{deploy_env}-landing.domain_payments_ops.BATCH_RUN_LOG`
    SET {', '.join(update_fields)}
    WHERE BATCH_RUN_LOG_UID = '{batch_run_log_uid}'
    """

    logging.info(update_statement)

    # Execute the update statement
    query_job = client.query(update_statement)

    try:
        query_job.result()
    except Exception:
        logger.error(query_job.errors)
        raise

    print(f"Execution entry with BATCH_RUN_LOG_UID: {batch_run_log_uid} updated successfully.")
