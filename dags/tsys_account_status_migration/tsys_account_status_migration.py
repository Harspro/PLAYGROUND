import logging
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflow import DAG, settings
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import util.constants as consts
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env_suffix,
)

logger = logging.getLogger(__name__)


gcp_config = read_variable_or_file(consts.GCP_CONFIG)
deploy_env = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
deploy_env_suffix = gcp_config[consts.DEPLOY_ENV_STORAGE_SUFFIX]


config_dir = f"{settings.DAGS_FOLDER}/tsys_account_status_migration/{consts.CONFIG}"
job_config = read_yamlfile_env_suffix(
    fname=f"{config_dir}/tsys_account_status_migration.yaml",
    deploy_env=deploy_env,
    env_suffix=deploy_env_suffix
)


dag_id = job_config[consts.DAG_ID]
dag_owner = job_config['dag_owner']

default_args = {
    "owner": "gremlins",
    'capability': 'platform-wide',
    'severity': 'P2',
    'sub_capability': 'status-maintenance',
    'business_impact': 'This will impact the account and card level status load"',
    'customer_impact': 'Customer login',
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True
}


def get_params(**context):
    limit = context['dag_run'].conf.get('limit', '')
    context['task_instance'].xcom_push(key='limit', value=limit)
    logger.info(f"limit passed: {limit}")


def get_conf_for_account_status(context, **kwargs):
    """Generate conf for account status kafka DAG"""
    ti = context['ti']
    limit = ti.xcom_pull(task_ids='get_params', key='limit')
    logger.info(f"Generating conf with limit: {limit}")
    return {
        'replacements': {
            "{limit}": limit if limit else ""
        }
    }


def get_conf_for_card_status(context, **kwargs):
    """Generate conf for card status kafka DAG"""
    ti = context['ti']
    limit = ti.xcom_pull(task_ids='get_params', key='limit')
    logger.info(f"Generating conf with limit: {limit}")
    return {
        'replacements': {
            "{limit}": limit if limit else ""
        }
    }


with DAG(
        dag_id=dag_id,
        description="Initial status load at account and card levels",
        tags=["gremlins"],
        schedule=None,
        start_date=datetime(2025, 6, 1),
        max_active_runs=1,
        catchup=False,
        is_paused_upon_creation=True,
        default_args=default_args
) as dag:
    start = EmptyOperator(task_id="start", trigger_rule='none_failed')
    done = EmptyOperator(task_id="done", trigger_rule='none_failed')

    get_params_task = PythonOperator(
        task_id='get_params',
        python_callable=get_params
    )

    send_to_kafka_account_status = TriggerDagRunOperator(
        task_id='send_to_kafka_account_status',
        trigger_dag_id='tsys_acc_master_status_bq_to_kafka',
        conf=get_conf_for_account_status,
        do_xcom_push=True,
        wait_for_completion=True,
        poke_interval=30
    )

    send_to_kafka_card_status = TriggerDagRunOperator(
        task_id='send_to_kafka_card_status',
        trigger_dag_id='tsys_card_level_status_bq_to_kafka',
        conf=get_conf_for_card_status,
        do_xcom_push=True,
        wait_for_completion=True,
        poke_interval=30
    )

    start >> get_params_task >> send_to_kafka_account_status >> send_to_kafka_card_status >> done
