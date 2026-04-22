import sys
from datetime import datetime

import pendulum
from airflow import DAG, settings
import os
import io
import json
import logging
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from google.cloud import bigquery
from airflow.operators.empty import EmptyOperator
from util.gcs_utils import read_file
import util.constants as consts
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env
)

logger = logging.getLogger(__name__)
gcp_config = read_variable_or_file(consts.GCP_CONFIG)
deploy_env = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
dag_config_fname = f"{settings.DAGS_FOLDER}/interac_processing/interac_inbound_dags_config.yaml"
dag_name = "interac_pcb_reconciliation_daily"
dag_config = read_yamlfile_env(f"{dag_config_fname}", deploy_env).get(dag_name)
if not dag_config:
    sys.exit("No tsys dags config file found")
destination_bq_dataset = dag_config.get("bq_dataset_id")
bq_table_name = dag_config.get(consts.TABLE_NAME)
region = dag_config.get("region")
headers_schema_file_path = "interac_processing/interac_recon_table_schema.json"
transactions_schema_file_path = "interac_processing/interac_recon_transactions_schema.json"
project_id = f"pcb-{deploy_env}-landing"
curated_project_id = f"pcb-{deploy_env}-curated"


def read_schema(schema_filename: str) -> list:
    print(f"curdir: {os.getcwd()}")
    dagsdir = settings.DAGS_FOLDER
    schema_filepath = f"{dagsdir}/{schema_filename}"
    if os.path.exists(schema_filepath):
        with open(schema_filepath, 'r') as schema_file:
            content = schema_file.read()
            schema_list = json.loads(content)
    else:
        logger.error(f"{schema_filepath} doesnt exist")
    return schema_list


def create_interac_recon_table(bq_destination_dataset_name, **context):
    headers_table = f"{project_id}.{bq_destination_dataset_name}.INTERAC_RECON_FILE_HEADERS"
    transactions_table = f"{project_id}.{bq_destination_dataset_name}.INTERAC_RECON_TRANSACTIONS"
    gcs_bucket_name = context[consts.DAG_RUN].conf[consts.BUCKET]
    file_path = context[consts.DAG_RUN].conf[consts.NAME]
    json_file_data = read_file(gcs_bucket_name, file_path)
    json_message = json.loads(json_file_data)
    transactions = json_message['bank_to_customer_account_report']['report']['entry']
    del json_message['bank_to_customer_account_report']['report']['entry']
    message_id = json_message[dag_config.get("bank_to_customer_account_report")][dag_config.get("group_header")][
        dag_config.get("message_identification")]
    from_date_time = json_message[dag_config.get("bank_to_customer_account_report")][dag_config.get("report")][
        dag_config.get("from_to_date")][dag_config.get("from_date_time")]
    to_date_time = json_message[dag_config.get("bank_to_customer_account_report")][dag_config.get("report")][
        dag_config.get("from_to_date")][dag_config.get("to_date_time")]
    context['ti'].xcom_push(key="message_id", value=message_id)
    context['ti'].xcom_push(key="from_date_time", value=from_date_time)
    context['ti'].xcom_push(key="to_date_time", value=to_date_time)

    logger.info("message_id= {}".format(message_id))
    logger.info("from_date_time= {}".format(from_date_time))
    logger.info("to_date_time= {}".format(to_date_time))

    transactions_delimited = "\n".join(
        json.dumps({**transaction, "message_id": message_id}) for transaction in transactions
    )

    headers_as_file = io.BytesIO(bytes(json.dumps(json_message).replace('\n', ''), 'utf-8'))
    transactions_as_file = io.BytesIO(bytes(transactions_delimited, 'utf-8'))
    load_to_bq(headers_as_file, headers_table, headers_schema_file_path)
    load_to_bq(transactions_as_file, transactions_table, transactions_schema_file_path)


def load_to_bq(data_as_file, table_name, schema_file_path):
    job_config = bigquery.LoadJobConfig(
        schema=read_schema(schema_file_path),
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]
    )
    load_job = bigquery.Client().load_table_from_file(
        data_as_file,
        table_name,
        location=region,
        job_config=job_config
    )
    try:
        load_job.result()
    except Exception:
        logger.error(load_job.errors)
        raise


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
    load_data_to_bigquery = PythonOperator(
        task_id="load_data_into_bq",
        python_callable=create_interac_recon_table,
        op_kwargs={"bq_destination_dataset_name": destination_bq_dataset},
        trigger_rule="all_success"
    )
    load_recon_exceptions = TriggerDagRunOperator(
        task_id="load_recon_exceptions",
        trigger_dag_id="interac_inbound_recon_exceptions",
        conf={
            "message_id": "{{ ti.xcom_pull(task_ids='load_data_into_bq', key='message_id') }}",
            "from_date_time": "{{ ti.xcom_pull(task_ids='load_data_into_bq', key='from_date_time') }}",
            "to_date_time": "{{ ti.xcom_pull(task_ids='load_data_into_bq', key='to_date_time') }}"
        },
        wait_for_completion=True,
        poke_interval=30,
    )

    end = EmptyOperator(task_id=consts.END_TASK_ID)

    start >> load_data_to_bigquery >> load_recon_exceptions >> end
