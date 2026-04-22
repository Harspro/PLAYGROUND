import logging
from datetime import timedelta, datetime
import pendulum
from airflow import DAG
from airflow import settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import util.constants as consts
from util.deploy_utils import read_pause_unpause_setting, pause_unpause_dag
from util.miscutils import read_variable_or_file, read_file_env
from util.bq_utils import run_bq_query
from airflow.providers.google.cloud.operators.bigquery import (BigQueryInsertJobOperator)
from google.cloud import bigquery
from airflow.utils.trigger_rule import TriggerRule
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)
gcp_config = read_variable_or_file(consts.GCP_CONFIG)
deploy_env_name = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
local_tz = pendulum.timezone('America/Toronto')

DAG_DEFAULT_ARGS = {
    'owner': 'team-digital-adoption-alerts',
    'capability': 'account-management',
    'severity': 'P3',
    'sub_capability': 'account-management',
    'business_impact': 'This DAG will identify the eligible accounts for the eve loyalty/margin rate offer',
    'customer_impact': 'Customer will miss out the eve loyalty/margin rate offer for the day',
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retry_delay': timedelta(seconds=10)
}


def check_for_active_offer():
    validation_query = read_file_env(f"{settings.DAGS_FOLDER}/digital_adoption/sql/loyalty_active_offer_validation_query.sql", deploy_env_name)
    result = run_bq_query(validation_query).result()
    count = list(result)[0]['record_count']
    if count > 0:
        return 'load_loyalty_offer_eligible_account'
    else:
        logger.info("Warning: No active Loyalty Offers")
        return 'branch_on_offer_status_change_existence'


def check_for_eligible_offer():
    validation_query = read_file_env(f"{settings.DAGS_FOLDER}/digital_adoption/sql/loyalty_eligible_offer_validation_query.sql", deploy_env_name)
    result = run_bq_query(validation_query).result()
    count = list(result)[0]['record_count']
    if count > 0:
        return 'kafka_writer_task_for_loyalty_offer'
    else:
        logger.info("Warning: No eligible Loyalty Offer Accounts for the day")
        return 'branch_on_offer_status_change_existence'


def check_for_status_change_offer():
    validation_query = read_file_env(f"{settings.DAGS_FOLDER}/digital_adoption/sql/loyalty_status_change_offer_validation_query.sql", deploy_env_name)
    result = run_bq_query(validation_query).result()
    count = list(result)[0]['record_count']
    if count > 0:
        return 'kafka_writer_task_for_loyalty_offer_status'
    else:
        logger.info("Warning: No Loyalty Offer Status changes for the day")
        return 'end'


with DAG(dag_id='loyalty_savings_offer_processing',
         default_args=DAG_DEFAULT_ARGS,
         schedule="15 0 * * *",
         description='DAG to identify the eligible accounts for loyalty offers ',
         start_date=datetime(2025, 5, 1, tzinfo=local_tz),
         max_active_runs=1,
         catchup=False,
         dagrun_timeout=timedelta(hours=24),
         is_paused_upon_creation=True
         ) as dag:

    add_tags(dag)

    is_paused = read_pause_unpause_setting('loyalty_savings_offer_processing', deploy_env_name)
    pause_unpause_dag(dag, is_paused)

    start_point = EmptyOperator(task_id=consts.START_TASK_ID)
    end_point = EmptyOperator(task_id=consts.END_TASK_ID, trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    check_for_active_offer_task = BranchPythonOperator(
        task_id='branch_on_record_existence',
        python_callable=check_for_active_offer
    )

    load_eligible_account_task = BigQueryInsertJobOperator(
        task_id="load_loyalty_offer_eligible_account",
        configuration={
            'query': {
                'query': read_file_env(f"{settings.DAGS_FOLDER}/digital_adoption/sql/loyalty_savings_offer.sql", deploy_env_name),
                'useLegacySql': False,
                'destinationTable': {
                    'projectId': f"pcb-{deploy_env_name}-landing",
                    'datasetId': 'domain_account_management',
                    'tableId': 'LOYALTY_OFFER_ELIGIBLE_ACCOUNT'
                },
                'timePartitioning': {
                    'type': 'DAY',
                    'field': 'REC_LOAD_TIMESTAMP'
                },
                "clustering": {
                    "fields": ["PCMA_ACCOUNT_ID"]
                },
                'writeDisposition': 'WRITE_APPEND'
            }
        },
        location=gcp_config.get('bq_query_location')
    )

    check_for_eligible_offer_task = BranchPythonOperator(
        task_id='branch_on_offer_existence',
        python_callable=check_for_eligible_offer
    )

    load_loyalty_savings_offer_table_task = BigQueryInsertJobOperator(
        task_id="load_loyalty_savings_offer_table",
        configuration={
            'query': {
                'query': read_file_env(f"{settings.DAGS_FOLDER}/digital_adoption/sql/loyalty_offer_account.sql", deploy_env_name),
                'useLegacySql': False,
                'destinationTable': {
                    'projectId': f"pcb-{deploy_env_name}-landing",
                    'datasetId': 'domain_account_management',
                    'tableId': 'LOYALTY_OFFER_ACCOUNT'
                },
                'timePartitioning': {
                    'type': 'DAY',
                    'field': 'INGESTION_TIMESTAMP'
                },
                "clustering": {
                    "fields": ["pcmaAccountId"]
                },
                'writeDisposition': 'WRITE_APPEND'
            }
        },
        location=gcp_config.get('bq_query_location')
    )

    loyalty_offer_kafka_writer_task = TriggerDagRunOperator(
        task_id='kafka_writer_task_for_loyalty_offer',
        trigger_dag_id='loyalty_offer_kafka_writer',
        wait_for_completion=True
    )

    check_for_offer_status_task = BranchPythonOperator(
        task_id='branch_on_offer_status_change_existence',
        python_callable=check_for_status_change_offer,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    loyalty_offer_status_kafka_writer_task = TriggerDagRunOperator(
        task_id='kafka_writer_task_for_loyalty_offer_status',
        trigger_dag_id='loyalty_offer_status_kafka_writer',
        wait_for_completion=True
    )

    start_point >> check_for_active_offer_task
    check_for_active_offer_task >> load_eligible_account_task >> load_loyalty_savings_offer_table_task >> check_for_eligible_offer_task
    check_for_active_offer_task >> check_for_offer_status_task >> loyalty_offer_status_kafka_writer_task >> end_point
    check_for_eligible_offer_task >> loyalty_offer_kafka_writer_task >> check_for_offer_status_task
    check_for_eligible_offer_task >> check_for_offer_status_task >> loyalty_offer_status_kafka_writer_task >> end_point
    check_for_offer_status_task >> loyalty_offer_status_kafka_writer_task >> end_point
    check_for_offer_status_task >> end_point
