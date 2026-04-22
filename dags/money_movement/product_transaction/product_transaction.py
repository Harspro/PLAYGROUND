import pendulum
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from util.miscutils import (
    read_variable_or_file,
    read_file_env
)
from datetime import datetime
import util.constants as consts
import logging
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)

gcp_config = read_variable_or_file(consts.GCP_CONFIG)
deploy_env = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
report_date_param = "{{params.report_date}}"

DAG_DEFAULT_ARGS = {
    "owner": "team-digital-adoption-alerts",
    "capability": "payments",
    "sub_capability": "EMT",
    "business_impact": "Product transaction table not generated for today",
    "customer_impact": "Product transaction table not generated for today",
    "severity": "P2",
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_exponential_backoff": True
}


def clear_current_day_query(report_date):
    return (f"DELETE FROM pcb-{deploy_env}-curated.domain_account_management.PRODUCT_TRANSACTION"
            f" WHERE POSTED_DATE = {report_date}")


def append_query(report_date):
    return (f"SELECT * FROM pcb-{deploy_env}-curated.domain_account_management.PRODUCT_TRANSACTION_VW"
            f" WHERE POSTED_DATE = {report_date}")


with DAG(
        dag_id='product_transaction',
        max_active_runs=1,
        default_args=DAG_DEFAULT_ARGS,
        schedule='0 3 * * TUE-SAT',
        start_date=datetime(2024, 8, 1, tzinfo=pendulum.timezone('America/Toronto')),
        is_paused_upon_creation=True,
        catchup=False,
        params={"report_date": "DATE_SUB(CURRENT_DATE('America/Toronto'), INTERVAL 1 DAY)"}
) as dag:

    add_tags(dag)

    clear_current_day = BigQueryInsertJobOperator(
        task_id='clear_current_day',
        configuration={
            'query': {
                'query': clear_current_day_query(report_date_param),
                'useLegacySql': False
            }
        },
        location=gcp_config.get('bq_query_location')
    )

    load_table = BigQueryInsertJobOperator(
        task_id='load_table_task',
        configuration={
            'query': {
                'query': append_query(report_date_param),
                'useLegacySql': False,
                'destinationTable': {
                    'projectId': f"pcb-{deploy_env}-curated",
                    'datasetId': 'domain_account_management',
                    'tableId': 'PRODUCT_TRANSACTION'
                },
                'timePartitioning': {
                    'type': 'DAY',
                    'field': 'POSTED_DATE'
                },
                'writeDisposition': 'WRITE_APPEND',
                'schemaUpdateOptions': ['ALLOW_FIELD_ADDITION'],
            }
        },
        location=gcp_config.get('bq_query_location')
    )
    clear_current_day >> load_table
