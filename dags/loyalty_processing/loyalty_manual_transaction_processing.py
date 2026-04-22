from datetime import datetime, timedelta

import pendulum
import logging
import util.constants as consts
from airflow import DAG
from airflow import settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env_suffix
)

logger = logging.getLogger(__name__)

gcp_config = read_variable_or_file(consts.GCP_CONFIG)
deploy_env = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
deploy_env_suffix = gcp_config['deploy_env_storage_suffix']
config_dir = f'{settings.DAGS_FOLDER}/loyalty_processing'

job_config = read_yamlfile_env_suffix(
    fname=f'{config_dir}/loyalty_manual_transaction_processing_config.yaml',
    deploy_env=deploy_env, env_suffix=deploy_env_suffix)

default_args = {
    'owner': 'team-convergence-alerts',
    'capability': 'loyalty',
    'severity': 'P3',
    'sub_capability': 'TBD',
    'business_impact': 'TBD',
    'customer_impact': 'TBD',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=2 * 60)
}


with DAG(
        dag_id=job_config['dag_id'],
        description='Manual Transaction Processing DAG',
        schedule='30 10 * * 1-5',
        default_args=default_args,
        catchup=False,
        is_paused_upon_creation=True,
        max_active_runs=1,
        start_date=pendulum.today('America/Toronto').add(days=-1)
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    trigger_merge_control_table_task = TriggerDagRunOperator(
        task_id=job_config['merge_control_table_task_id'],
        trigger_dag_id=job_config['merge_control_table_dag_id'],
        wait_for_completion=True,
        poke_interval=30,
        logical_date=datetime.now(tz=pendulum.timezone('America/Toronto'))
    )

    trigger_manual_transaction_kafka_writer_dag = TriggerDagRunOperator(
        task_id=job_config['kafka_writer_task_id'],
        trigger_dag_id=job_config['kafka_writer_dag_id'],
        wait_for_completion=True,
        poke_interval=30,
        logical_date=datetime.now(tz=pendulum.timezone('America/Toronto'))
    )

    trigger_update_table_dag = TriggerDagRunOperator(
        task_id=job_config['update_table_task_id'],
        trigger_dag_id=job_config['update_table_dag_id'],
        wait_for_completion=True,
        poke_interval=30,
        logical_date=datetime.now(tz=pendulum.timezone('America/Toronto'))
    )

    start >> trigger_merge_control_table_task >> trigger_manual_transaction_kafka_writer_dag >> trigger_update_table_dag >> end
