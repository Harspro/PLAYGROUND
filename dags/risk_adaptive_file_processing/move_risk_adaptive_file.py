import pendulum
import util.constants as consts
from datetime import timedelta
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow import DAG
from airflow import settings
from airflow.operators.empty import EmptyOperator
from util.miscutils import (
    read_yamlfile_env_suffix,
    read_variable_or_file
)

gcp_config = read_variable_or_file(consts.GCP_CONFIG)
deploy_env = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
config_dir = f'{settings.DAGS_FOLDER}/risk_adaptive_file_processing/{consts.CONFIG}'
deploy_env_suffix = gcp_config['deploy_env_storage_suffix']

job_config = read_yamlfile_env_suffix(
    fname=f'{config_dir}/risk_adaptive_file_config.yaml',
    deploy_env=deploy_env, env_suffix=deploy_env_suffix)

dag_id = "move_risk_adaptive_file"
source_bucket = "{{ dag_run.conf.get('source_bucket')}}"
source_object = "{{ dag_run.conf.get('source_object')}}"
destination_bucket = "{{ dag_run.conf.get('destination_bucket')}}"
destination_object = "{{ dag_run.conf.get('destination_object')}}"

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
    'retries': 5,
    'retry_delay': timedelta(days=1)
}


severity_tag = default_args['severity']

with DAG(
        dag_id=dag_id,
        description='Risk Adapative File Processing DAG',
        schedule=None,
        default_args=default_args,
        tags=[severity_tag],
        catchup=False,
        is_paused_upon_creation=True,
        max_active_runs=1,
        start_date=pendulum.datetime(2023, 11, 27, tz='America/Toronto')
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    move_risk_adaptive_file_to_landing = GCSToGCSOperator(
        task_id='move_risk_adaptive_file_to_landing',
        gcp_conn_id=gcp_config.get("processing_zone_connection_id"),
        source_bucket=source_bucket,
        source_object=source_object,
        destination_bucket=destination_bucket,
        destination_object=destination_object,
        dag=dag,
    )

    start >> move_risk_adaptive_file_to_landing >> end
