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
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)

gcp_config = read_variable_or_file(consts.GCP_CONFIG)
deploy_env = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
sql_file_name = f"{settings.DAGS_FOLDER}/money_movement/product_transact_account_eod/product_transact_account_eod.sql"
report_date_param = "{{params.report_date}}"

DAG_DEFAULT_ARGS = {
    "owner": "team-digital-adoption-alerts",
    "capability": "payments",
    "sub_capability": "EMT",
    "business_impact": "Daily product transact account eod not generated",
    "customer_impact": "Daily product transact account eod not generated",
    "severity": "P2",
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_exponential_backoff": True
}


def sql(report_date):
    return read_file_env(sql_file_name, deploy_env).replace('<<<report_date_goes_here>>>', report_date)


def clear_current_day_query(report_date):
    return f"DELETE FROM pcb-{deploy_env}-curated.domain_account_management.PRODUCT_TRANSACT_ACCOUNT_EOD_DETAIL WHERE DATE(RECORD_LOAD_TIMESTAMP) = {report_date}"


with DAG(
        dag_id='product_transact_account_eod',
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
                'query': sql(report_date_param),
                'useLegacySql': False,
                'destinationTable': {
                    'projectId': f'pcb-{deploy_env}-curated',
                    'datasetId': 'domain_account_management',
                    'tableId': 'PRODUCT_TRANSACT_ACCOUNT_EOD_DETAIL'
                },
                'timePartitioning': {
                    'type': 'DAY',
                    'field': 'RECORD_LOAD_TIMESTAMP'
                },
                'writeDisposition': 'WRITE_APPEND',
                'schemaUpdateOptions': ['ALLOW_FIELD_ADDITION']
            }
        },
        location=gcp_config.get('bq_query_location')
    )
    clear_current_day >> load_table
