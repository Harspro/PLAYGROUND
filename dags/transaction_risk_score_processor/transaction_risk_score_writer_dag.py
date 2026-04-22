import json
import logging
from datetime import datetime, timedelta
from typing import Final
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import pendulum
import pytz
from airflow import DAG, settings
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator
)
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.state import State

import util.constants as consts
from util.miscutils import (
    read_variable_or_file,
    get_cluster_name,
    get_cluster_config_by_job_size,
    read_yamlfile_env_suffix,
)
from util.gcs_utils import (
    read_file,
)

logger = logging.getLogger(__name__)

# Load configurations
gcp_config = read_variable_or_file(consts.GCP_CONFIG)
deploy_env = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
deploy_env_suffix = gcp_config[consts.DEPLOY_ENV_STORAGE_SUFFIX]
dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)

# Set directory paths and load job configuration
config_dir = f"{settings.DAGS_FOLDER}/transaction_risk_score_processor/{consts.CONFIG}"
job_config = read_yamlfile_env_suffix(
    fname=f"{config_dir}/trs_processing_writer_config.yaml",
    deploy_env=deploy_env,
    env_suffix=deploy_env_suffix
)

# Extract configuration values
dag_id = job_config[consts.DAG_ID]
ephemeral_cluster_name = get_cluster_name(True, dataproc_config, dag_id=dag_id)
dag_owner = job_config['dag_owner']
curated_project_id = gcp_config.get(consts.CURATED_ZONE_PROJECT_ID)
domain_scoring_dataset_id = job_config['domain_scoring_dataset']
bq_curated_table_trs_input = job_config['bq_curated_table_trs_input']
trs_approval_inbound_folder = "trs"
trs_approval_inbound_approved_folder = "batch_approvals/archive/trs/approved/"
trs_approval_inbound_rejected_folder = "batch_approvals/archive/trs/rejected/"
input_bucket = job_config['input_bucket']
output_bucket = job_config['outbound_bucket']
staging_bucket = job_config['staging_bucket']
processing_output_dir = "trs_processing/output/"
trs_approval_file_basepath: Final = "trs/"
file_extension = '.prod' if deploy_env == "prod" else '.uatv'

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


def trigger_spark_writer_job_task() -> dict:
    """
    Build the configuration for the Java Spark Writer job.

    Returns:
        dict: Job configuration dictionary
    """
    dag_info = f"[dag_name: {dag_id}, dag_run_id: '{{run_id}}', dag_owner: {dag_owner}]"
    logger.info('Building Writer Spark job configuration')
    execution_id = "{{ ti.xcom_pull(task_ids='read_approval_file', key='execution_id') }}"
    output_filename = "{{ ti.xcom_pull(task_ids='read_approval_file', key='output_filename') }}"

    return {
        consts.REFERENCE: {
            consts.PROJECT_ID: dataproc_config.get(consts.PROJECT_ID)
        },
        consts.PLACEMENT: {
            consts.CLUSTER_NAME: ephemeral_cluster_name
        },
        consts.SPARK_JOB: {
            consts.JAR_FILE_URIS: job_config[consts.JAR_FILE_URIS],
            consts.MAIN_CLASS: job_config[consts.MAIN_CLASS],
            consts.ARGS: [
                f'project={curated_project_id}',
                f'dataset={domain_scoring_dataset_id}',
                f'table={bq_curated_table_trs_input}',
                f'execution.id={execution_id}',
                f'file.name={output_filename}',
                'output.filepath=' + f'gs://{staging_bucket}/{processing_output_dir}',
                f"{consts.SPARK_DAG_INFO}={dag_info}"
            ]
        }
    }


def read_approval_file(**context):
    file_name = context['dag_run'].conf.get('file_name')
    execution_id = file_name[:-5]
    output_filename = 'pcb_tsys_pcmc_cust_trs_' + datetime.now().astimezone(pytz.timezone('America/Toronto')).strftime('%Y%m%d%H%M%S')[:14] + file_extension

    context['task_instance'].xcom_push(key='file_name', value=file_name)
    context['task_instance'].xcom_push(key='execution_id', value=execution_id)
    context['task_instance'].xcom_push(key='output_filename', value=output_filename)

    logger.info(f"FileName Received: {file_name}")
    logger.info(f"Actual FileName: {execution_id}")

    json_data = read_file(input_bucket, trs_approval_file_basepath + file_name)
    approval_status = json.loads(json_data).get("approval_status")

    if approval_status is True:
        return 'move_file_to_archive_approved'
    else:
        return 'move_file_to_archive_rejected'


# Function to check status of upstream task
def check_multiple_upstream_task_status(**context):
    for task_instance in context['dag_run'].get_task_instances():
        if task_instance.task_id == 'move_file_to_outbound_landing' and task_instance.state == (State.SUCCESS):
            logger.info('move_file_to_outbound_landing ran successfullly. A status of FILE_CREATED will be sent to the Kafka topic.')
            context['task_instance'].xcom_push(key='fileStatus', value='FILE_CREATED')
        elif task_instance.task_id == 'move_file_to_outbound_landing' and task_instance.state != (State.SUCCESS):
            logger.info('move_file_to_outbound_landing ran with failure state. A status of FILE_CREATE_FAILED will be sent to the Kafka topic.')
            context['task_instance'].xcom_push(key='fileStatus', value='FILE_CREATE_FAILED')
        else:
            logger.info(f"Task: {task_instance.task_id} is in {task_instance.state} state.")


severity_tag = default_args['severity']
# DAG definition
with DAG(
    dag_id=dag_id,
    description='TRS File Writer DAG',
    default_args=default_args,
    tags=[severity_tag],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    schedule=None,
    start_date=pendulum.datetime(2023, 11, 27, tz='America/Toronto')
) as dag:

    read_approval_file_task = BranchPythonOperator(
        task_id='read_approval_file',
        python_callable=read_approval_file,
        trigger_rule='one_success',
    )

    move_file_to_archive_approved = GCSToGCSOperator(
        task_id='move_file_to_archive_approved',
        gcp_conn_id=gcp_config.get("landing_zone_connection_id"),
        source_bucket=input_bucket,
        source_object=f"{trs_approval_inbound_folder}/{{{{ ti.xcom_pull(task_ids='read_approval_file', key='file_name') }}}}",
        destination_bucket=input_bucket,
        destination_object=f"{trs_approval_inbound_approved_folder}{{{{ ti.xcom_pull(task_ids='read_approval_file', key='file_name') }}}}",
        move_object=True,
    )

    move_file_to_archive_rejected = GCSToGCSOperator(
        task_id='move_file_to_archive_rejected',
        gcp_conn_id=gcp_config.get("landing_zone_connection_id"),
        source_bucket=input_bucket,
        source_object=f"{trs_approval_inbound_folder}/{{{{ ti.xcom_pull(task_ids='read_approval_file', key='file_name') }}}}",
        destination_bucket=input_bucket,
        destination_object=f"{trs_approval_inbound_rejected_folder}{{{{ ti.xcom_pull(task_ids='read_approval_file', key='file_name') }}}}",
        move_object=True,
    )

    create_spark_cluster_task = DataprocCreateClusterOperator(
        task_id=consts.CLUSTER_CREATING_TASK_ID,
        project_id=dataproc_config.get(consts.PROJECT_ID),
        cluster_config=get_cluster_config_by_job_size(
            deploy_env,
            gcp_config.get(consts.NETWORK_TAG),
            'small'
        ),
        region=dataproc_config.get(consts.LOCATION),
        cluster_name=ephemeral_cluster_name,
    )

    trigger_jspark_writer_job_task = DataprocSubmitJobOperator(
        task_id="trigger_spark_writer_job_task",
        job=trigger_spark_writer_job_task(),
        region=dataproc_config.get(consts.LOCATION),
        project_id=dataproc_config.get(consts.PROJECT_ID),
        gcp_conn_id=gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID)
    )

    move_file_to_outbound_landing = GCSToGCSOperator(
        task_id='move_file_to_outbound_landing',
        gcp_conn_id=gcp_config.get("processing_zone_connection_id"),
        source_bucket=staging_bucket,
        source_object=f"{processing_output_dir}{{{{ ti.xcom_pull(task_ids='read_approval_file', key='output_filename') }}}}",
        destination_bucket=output_bucket,
        destination_object="{{ ti.xcom_pull(task_ids='read_approval_file', key='output_filename') }}",
        move_object=True
    )

    # Task to check the status of multiple upstream tasks
    trigger_file_status_kafka_writer_dag = PythonOperator(
        task_id='trigger_file_status_kafka_writer_dag',
        python_callable=check_multiple_upstream_task_status,
        trigger_rule=TriggerRule.NONE_SKIPPED,
    )

    vendor_files_status_message_kafka_dag = TriggerDagRunOperator(
        task_id='vendor_files_status_message_kafka_dag',
        trigger_dag_id='transaction_risk_score_writer_file_status_kafka_writer',
        conf={
            'replacements': {
                "{file_status}": "{{ ti.xcom_pull(task_ids='trigger_file_status_kafka_writer_dag', key='fileStatus') }}",
                "{execution_id}": "{{ ti.xcom_pull(task_ids='read_approval_file', key='execution_id') }}",
                "{file_name}": "{{ ti.xcom_pull(task_ids='read_approval_file', key='output_filename') }}"
            }},
        wait_for_completion=False,
        poke_interval=30
    )

    # Set task dependencies
    read_approval_file_task >> move_file_to_archive_rejected
    read_approval_file_task >> move_file_to_archive_approved >> create_spark_cluster_task >> trigger_jspark_writer_job_task >> move_file_to_outbound_landing >> trigger_file_status_kafka_writer_dag >> vendor_files_status_message_kafka_dag
