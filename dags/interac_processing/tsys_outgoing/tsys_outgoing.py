import pendulum
from airflow import DAG, settings
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from util.miscutils import (
    read_variable_or_file,
    read_file_env
)
from datetime import datetime
import util.constants as consts
import logging

logger = logging.getLogger(__name__)

gcp_config = read_variable_or_file(consts.GCP_CONFIG)
deploy_env = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
sql_file_name = f"{settings.DAGS_FOLDER}/interac_processing/tsys_outgoing/tsys_outgoing.sql"
sql = read_file_env(sql_file_name, deploy_env)

DAG_DEFAULT_ARGS = {
    "owner": "team-money-movement-eng",
    "capability": "Payments",
    "severity": "P2",
    "sub_capability": "EMT",
    "business_impact": "Daily tsys outgoing report not generated.",
    "customer_impact": "Tsys outgoing report not available to business and thus not actioned",
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_exponential_backoff": True
}

with DAG(
        dag_id='tsys_outgoing',
        max_active_runs=1,
        default_args=DAG_DEFAULT_ARGS,
        schedule='0 8 * * MON-FRI',
        start_date=datetime(2024, 1, 4, tzinfo=pendulum.timezone('America/Toronto')),
        is_paused_upon_creation=True,
        catchup=False
) as dag:
    load__table = BigQueryInsertJobOperator(
        task_id='load_table_task',
        configuration={
            'query': {
                'query': sql,
                'useLegacySql': False,
                'destinationTable': {
                    'projectId': f"pcb-{deploy_env}-curated",
                    'datasetId': 'domain_payments',
                    'tableId': 'TSYS_OUTGOING'
                },
                'timePartitioning': {
                    'type': 'DAY',
                    'field': 'REPORT_DATE'
                },
                'writeDisposition': 'WRITE_APPEND'
            }
        },
        location=gcp_config.get('bq_query_location')
    )
