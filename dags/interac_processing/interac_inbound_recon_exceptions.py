import sys
from datetime import datetime
import pendulum
from airflow import DAG, settings
import logging
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from airflow.operators.empty import EmptyOperator
import util.constants as consts
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env,
    read_file_env
)

DEFAULT_WINDOW = "2"
logger = logging.getLogger(__name__)
gcp_config = read_variable_or_file(consts.GCP_CONFIG)
deploy_env = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
dag_config_fname = f"{settings.DAGS_FOLDER}/interac_processing/interac_inbound_dags_config.yaml"
dag_name = "interac_inbound_recon_exceptions"
dag_config = read_yamlfile_env(f"{dag_config_fname}", deploy_env).get(dag_name)
if not dag_config:
    sys.exit("No tsys dags config file found")
destination_bq_dataset = dag_config.get("bq_dataset_id")
bq_table_name = dag_config.get(consts.TABLE_NAME)
sql_file_name = f"{settings.DAGS_FOLDER}/" + dag_config.get("sql_file_path")
cleanup_sql_file_name = f"{settings.DAGS_FOLDER}/" + dag_config.get("cleanup_sql_file_path")
bq_recon_curated_table_name = dag_config.get("curated_table_name")
region = dag_config.get("region")
schema_file_path = "interac_processing/interac_recon_table_schema.json"
project_id = f"pcb-{deploy_env}-landing"
curated_project_id = f"pcb-{deploy_env}-curated"


def cleanup_recon_table(bq_destination_dataset_name, cleanup_sql_file_name, curated_table_name, **context):
    client = bigquery.Client()
    table_id = f"{curated_project_id}.{bq_destination_dataset_name}.{curated_table_name}"
    from_date_time = context["dag_run"].conf["from_date_time"]
    logger.info("from_date_time is: {}".format(from_date_time))
    sql = read_file_env(cleanup_sql_file_name, deploy_env).replace("<<<from_timestamp_goes_here>>>", f'"{from_date_time}"')
    logger.info("sql is: {}".format(sql))
    query_job = client.query(sql)
    query_job.result()
    logger.info("Pre-loading cleaup done for the table {}".format(table_id))


def load_recon_table(bq_destination_dataset_name, sql_file_name, curated_table_name, **context):
    client = bigquery.Client()
    table_id = f"{curated_project_id}.{bq_destination_dataset_name}.{curated_table_name}"
    job_config = bigquery.QueryJobConfig(destination=table_id)
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    message_id = context["dag_run"].conf["message_id"]
    from_date_time = context["dag_run"].conf["from_date_time"]
    to_date_time = context["dag_run"].conf["to_date_time"]
    time_window_days = context["dag_run"].conf.get("time_window_days") or DEFAULT_WINDOW

    logger.info("message_id is: {}".format(message_id))
    logger.info("from_date_time is: {}".format(from_date_time))
    logger.info("to_date_time is: {}".format(to_date_time))
    logger.info("time_window_days is: {}".format(time_window_days))

    sql = (read_file_env(sql_file_name, deploy_env)
           .replace("<<<from_timestamp_goes_here>>>", f'"{from_date_time}"')
           .replace("<<<to_timestamp_goes_here>>>", f'"{to_date_time}"')
           .replace("<<<file_id_goes_here>>>", f'"{message_id}"')
           .replace("<<<time_window_days>>>", time_window_days))
    query_job = client.query(sql, job_config=job_config)
    query_job.result()
    logger.info("Query results loaded to the table {}".format(table_id))


DAG_DEFAULT_ARGS = {
    "owner": "team-money-movement-eng",
    "capability": "Payments",
    "severity": "P2",
    "sub_capability": "EMT",
    "business_impact": "Daily Interac transactions are not reconciled",
    "customer_impact": "Interac transactions exceptions not reported to business and thus not actioned",
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_exponential_backoff": True
}

with DAG(dag_id=dag_name,
         schedule=None,
         start_date=datetime(2023, 1, 1, tzinfo=pendulum.timezone('America/Toronto')),
         max_active_runs=1,
         is_paused_upon_creation=True,
         catchup=False,
         default_args=DAG_DEFAULT_ARGS) as dag:
    start = EmptyOperator(task_id=consts.START_TASK_ID)
    clenaup_recon_table = PythonOperator(
        task_id="cleanup_recon_table_task",
        python_callable=cleanup_recon_table,
        op_kwargs={"bq_destination_dataset_name": destination_bq_dataset, "cleanup_sql_file_name": cleanup_sql_file_name, "curated_table_name": bq_recon_curated_table_name},
        trigger_rule="all_success"
    )
    load_recon_exceptions = PythonOperator(
        task_id="load_recon_table_task",
        python_callable=load_recon_table,
        op_kwargs={"bq_destination_dataset_name": destination_bq_dataset, "sql_file_name": sql_file_name, "curated_table_name": bq_recon_curated_table_name},
        trigger_rule="all_success"
    )
    end = EmptyOperator(task_id=consts.END_TASK_ID)

    start >> clenaup_recon_table >> load_recon_exceptions >> end
