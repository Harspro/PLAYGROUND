import logging
from datetime import datetime, timedelta
from typing import Final

import pendulum
from airflow import DAG, settings
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.operators.empty import EmptyOperator

import util.constants as consts
from util.miscutils import (
    read_variable_or_file,
    get_cluster_name,
    get_cluster_config_by_job_size,
    read_yamlfile_env_suffix,
)

logger = logging.getLogger(__name__)

# Load configurations
gcp_config = read_variable_or_file(consts.GCP_CONFIG)
deploy_env = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
deploy_env_suffix = gcp_config[consts.DEPLOY_ENV_STORAGE_SUFFIX]

# Set directory paths and load job configuration
config_dir = f"{settings.DAGS_FOLDER}/transaction_risk_score_processor/{consts.CONFIG}"
job_config = read_yamlfile_env_suffix(
    fname=f"{config_dir}/transaction_risk_score_bucket_sync_config.yaml",
    deploy_env=deploy_env,
    env_suffix=deploy_env_suffix
)

# Extract configuration values
dag_owner = job_config['dag_owner']
input_bucket = job_config['input_bucket']
output_bucket = job_config['output_bucket']
source_object_prefix = "trs/trs_exec_"

default_args = {
    'owner': 'team-defenders-alerts',
    'capability': 'risk-management',
    'severity': 'P2',
    'sub_capability': 'credit-risk',
    'business_impact': 'Bank Loss Risk will increase',
    'customer_impact': 'N/A',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=120)
}

severity_tag = default_args['severity']


with DAG(
        dag_id=job_config['dag_id'],
        description='TRS File Moving DAG',
        schedule=job_config['schedule'],
        default_args=default_args,
        tags=[severity_tag],
        catchup=False,
        is_paused_upon_creation=True,
        max_active_runs=1,
        start_date=pendulum.datetime(2024, 11, 14, tz='America/Toronto')
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    move_task = GCSToGCSOperator(
        task_id='move_file_to_eq_bucket',
        gcp_conn_id=gcp_config.get("landing_zone_connection_id"),
        source_bucket=input_bucket,
        source_object=f"{source_object_prefix}",
        destination_bucket=f"{output_bucket}",
        destination_object="trs/",
        match_glob="**/*",
        move_object=True
    )

    start >> move_task >> end
