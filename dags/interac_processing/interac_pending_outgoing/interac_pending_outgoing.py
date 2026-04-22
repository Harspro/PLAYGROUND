import pendulum
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from util.miscutils import (
    read_variable_or_file,
    read_file_env
)
from datetime import timedelta, datetime
from util.datetime_utils import current_datetime_toronto
import util.constants as consts
import logging
from vendor_holiday.util.utils import is_holiday

logger = logging.getLogger(__name__)

gcp_config = read_variable_or_file(consts.GCP_CONFIG)
deploy_env = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
sql_file_name = f"{settings.DAGS_FOLDER}/interac_processing/interac_pending_outgoing/interac_pending_outgoing.sql"
sql = read_file_env(sql_file_name, deploy_env)

DAG_DEFAULT_ARGS = {
    "owner": "team-money-movement-eng",
    "capability": "Payments",
    "severity": "P2",
    "sub_capability": "EMT",
    "business_impact": "Daily pending outgoing interac report not generated.",
    "customer_impact": "Pending outgoing interac report not available to business and thus not actioned",
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_exponential_backoff": True
}


def get_previous_weekday():
    today = current_datetime_toronto().date()
    previous_day = today - timedelta(days=1)
    # Check if the previous day was a weekday (Monday-Friday)
    while previous_day.weekday() > 4:  # 5 is for Saturday
        previous_day -= timedelta(days=1)
    return previous_day


def is_business_day():
    """
    Checks if the previous weekday day is a holiday.
    If previous weekday is a holiday, then no need to generate the report
    """
    if is_holiday(get_previous_weekday(), 'tsys'):
        return 'end'
    else:
        return 'load_pending_outgoing_table_task'


with DAG(
        dag_id='interac_pending_outgoing',
        max_active_runs=1,
        default_args=DAG_DEFAULT_ARGS,
        schedule='0 8 * * MON-FRI',
        start_date=datetime(2024, 1, 4, tzinfo=pendulum.timezone('America/Toronto')),
        is_paused_upon_creation=True,
        catchup=False
) as dag:
    check_business_day = BranchPythonOperator(
        task_id='check_business_day',
        python_callable=is_business_day
    )

    load_pending_outgoing_table = BigQueryInsertJobOperator(
        task_id='load_pending_outgoing_table_task',
        configuration={
            'query': {
                'query': sql,
                'useLegacySql': False,
                'destinationTable': {
                    'projectId': f"pcb-{deploy_env}-curated",
                    'datasetId': 'domain_payments',
                    'tableId': 'INTERAC_PENDING_OUTGOING'
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

    end = EmptyOperator(
        task_id='end'
    )

    check_business_day >> [load_pending_outgoing_table, end]
