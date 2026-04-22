import logging
from datetime import timedelta, datetime
import pendulum
from airflow import DAG
from airflow import settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import util.constants as consts
from util.deploy_utils import read_pause_unpause_setting, pause_unpause_dag
from util.miscutils import read_variable_or_file, read_file_env
from tokenized_consent_capture.external_task_status_pokesensor import ExternalTaskPokeSensor
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)
gcp_config = read_variable_or_file(consts.GCP_CONFIG)
deploy_env_name = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
local_tz = pendulum.timezone('America/Toronto')


DAG_DEFAULT_ARGS = {
    'owner': 'team-digital-adoption-alerts',
    'capability': 'account-management',
    'severity': 'P4',
    'sub_capability': 'account-management',
    'business_impact': 'This DAG will send eligible customers to a Kafka topic. If DAG fails records will not be sent.',
    'customer_impact': 'Customer will miss card upgrade offer.',
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retry_delay': timedelta(seconds=10)
}


with DAG(dag_id='product_upgrade_offer_kafka_writer',
         default_args=DAG_DEFAULT_ARGS,
         schedule="0 1 1 * *",
         description='DAG to send the product upgrade offers to DP Kafka',
         start_date=datetime(2025, 5, 1, tzinfo=local_tz),
         max_active_runs=1,
         catchup=False,
         dagrun_timeout=timedelta(hours=24),
         is_paused_upon_creation=True
         ) as dag:

    add_tags(dag)

    is_paused = read_pause_unpause_setting('product_upgrade_offer_kafka_writer', deploy_env_name)
    pause_unpause_dag(dag, is_paused)

    start_point = EmptyOperator(task_id=consts.START_TASK_ID)
    end_point = EmptyOperator(task_id=consts.END_TASK_ID)

    load_main_table_task = BigQueryInsertJobOperator(
        task_id="load_product_upgrade_offer_table",
        configuration={
            'query': {
                'query': read_file_env(f"{settings.DAGS_FOLDER}/digital_adoption/sql/product_upgrade_offers.sql", deploy_env_name),
                'useLegacySql': False,
                'destinationTable': {
                    'projectId': f"pcb-{deploy_env_name}-landing",
                    'datasetId': 'domain_account_management',
                    'tableId': 'PRODUCT_UPGRADE_OFFER'
                },
                'timePartitioning': {
                    'type': 'DAY',
                    'field': 'OFFER_CREATED_DT'
                },
                'writeDisposition': 'WRITE_APPEND'
            }
        },
        location=gcp_config.get('bq_query_location')
    )

    load_ref_table_task = BigQueryInsertJobOperator(
        task_id='load_ref_customer_offer_detail_table',
        configuration={
            'query': {
                'query': read_file_env(f"{settings.DAGS_FOLDER}/digital_adoption/sql/customer_offer_detail.sql", deploy_env_name),
                'useLegacySql': False,
            }
        },
        location=gcp_config.get('bq_query_location')
    )

    kafka_writer_task = TriggerDagRunOperator(
        task_id='kafka_writer_task_for_product_upgrade_offers',
        trigger_dag_id='product_upgrade_offers_kafka',
        wait_for_completion=True
    )

    start_point >> load_ref_table_task >> load_main_table_task >> kafka_writer_task >> end_point
