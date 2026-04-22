import logging
from datetime import timedelta
import pendulum
import util.constants as consts
from airflow import DAG
from airflow import settings
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator, DataprocCreateClusterOperator
from util.miscutils import (
    read_variable_or_file,
    get_cluster_name,
    get_ephemeral_cluster_config,
    read_yamlfile_env_suffix
)

logger = logging.getLogger(__name__)

gcp_config = read_variable_or_file(consts.GCP_CONFIG)
deploy_env = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)
config_dir = f'{settings.DAGS_FOLDER}/equifax_processing/{consts.CONFIG}'

deploy_env_suffix = gcp_config['deploy_env_storage_suffix']
job_config = read_yamlfile_env_suffix(
    fname=f'{config_dir}/equifax_processing_config.yaml',
    deploy_env=deploy_env, env_suffix=deploy_env_suffix)

dag_id = job_config['dag_id']
ephemeral_cluster_name = get_cluster_name(True, dataproc_config, dag_id=dag_id)
input_bucket = job_config['input_bucket']
output_bucket = job_config['output_bucket']
dag_owner = job_config['dag_owner']
staging_bucket = job_config['staging_bucket']
file_name_prefix = len(job_config["output_file_name_prefix"])
equifax_score_gen_folder = "equifax_score_gen"
processing_input_dir = "equifax_processing/equifax_score_gen/input/"
processing_interim_dir = "equifax_processing/equifax_score_gen/interim/"
processing_output_dir = "equifax_processing/equifax_score_gen/output/"


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
    'retries': 1,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': pendulum.datetime(2022, 1, 1, tz='America/Toronto')
}


def check_month_process_file(**context):
    file_name = context['dag_run'].conf.get('file_name')
    final_file = file_name[:file_name_prefix] + "_" + file_name[file_name_prefix + 1:]

    context['task_instance'].xcom_push(key='input_filename', value=file_name)
    context['task_instance'].xcom_push(key='output_filename', value=final_file)

    month = file_name[35:37]

    logger.info(f"Input file path: {file_name}")
    logger.info(f"Output file path: {final_file}")

    logger.info(f"MONTH: {month}")

    if month in job_config['month_list']:
        return 'move_file_to_processing_zone'
    else:
        return 'move_equifax_to_outbound'


def _build_fetching_job():
    dag_info = f"[dag_name: {dag_id}, dag_run_id: {'{{run_id}}'}, dag_owner: {dag_owner}]"
    input_filename = "{{ ti.xcom_pull(task_ids='check_month_process_file', key='output_filename') }}"

    return {
        consts.REFERENCE: {consts.PROJECT_ID: dataproc_config.get(consts.PROJECT_ID)},
        consts.PLACEMENT: {consts.CLUSTER_NAME: ephemeral_cluster_name},
        consts.SPARK_JOB: {
            consts.JAR_FILE_URIS: job_config[consts.JAR_FILE_URIS],
            consts.FILE_URIS: job_config[consts.FILE_URIS],
            consts.MAIN_CLASS: job_config[consts.MAIN_CLASS],
            consts.ARGS: ['input.filepath=' + f'gs://{staging_bucket}/{processing_input_dir}{input_filename}',
                          'interim.filepath=' + f'gs://{staging_bucket}/{processing_interim_dir}',
                          'output.filepath=' + f'gs://{staging_bucket}/{processing_output_dir}',
                          'config.filepath=application.yaml',
                          f'{consts.SPARK_DAG_INFO}={dag_info}'
                          ]
        }
    }


severity_tag = default_args['severity']


with DAG(
        dag_id=dag_id,
        description='Equifax_File_Processing',
        schedule=None,
        default_args=default_args,
        tags=[severity_tag],
        catchup=False,
        is_paused_upon_creation=True
) as dag:

    check_month_process_file = BranchPythonOperator(
        task_id='check_month_process_file',
        python_callable=check_month_process_file,
        trigger_rule='one_success',
    )

    move_equifax_inbound_to_outbound_bucket = GCSToGCSOperator(
        task_id='move_equifax_to_outbound',
        gcp_conn_id=gcp_config.get("landing_zone_connection_id"),
        source_bucket=input_bucket,
        source_object=f"{equifax_score_gen_folder}/{{{{ ti.xcom_pull(task_ids='check_month_process_file', key='input_filename') }}}}",
        destination_bucket=output_bucket,
        destination_object="{{ ti.xcom_pull(task_ids='check_month_process_file', key='output_filename') }}",
        dag=dag
    )

    move_file_to_processing_zone = GCSToGCSOperator(
        task_id='move_file_to_processing_zone',
        gcp_conn_id=gcp_config.get("landing_zone_connection_id"),
        source_bucket=input_bucket,
        source_object=f"{equifax_score_gen_folder}/{{{{ ti.xcom_pull(task_ids='check_month_process_file', key='input_filename') }}}}",
        destination_bucket=staging_bucket,
        destination_object=f"{processing_input_dir}{{{{ ti.xcom_pull(task_ids='check_month_process_file', key='output_filename') }}}}",
        dag=dag
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id=consts.CLUSTER_CREATING_TASK_ID,
        project_id=dataproc_config.get(consts.PROJECT_ID),
        cluster_config=get_ephemeral_cluster_config(deploy_env, gcp_config.get(consts.NETWORK_TAG), 3),
        region=dataproc_config.get(consts.LOCATION),
        cluster_name=ephemeral_cluster_name,
    )

    submit_java_spark_job = DataprocSubmitJobOperator(
        task_id="submit_java_spark_job",
        job=_build_fetching_job(),
        region=dataproc_config.get(consts.LOCATION),
        project_id=dataproc_config.get(consts.PROJECT_ID),
        gcp_conn_id=gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID),
    )

    move_file_to_outbound_landing = GCSToGCSOperator(
        task_id='move_file_to_outbound_landing',
        gcp_conn_id=gcp_config.get("processing_zone_connection_id"),
        source_bucket=staging_bucket,
        source_object=f"{processing_output_dir}{{{{ ti.xcom_pull(task_ids='check_month_process_file', key='output_filename') }}}}",
        destination_bucket=output_bucket,
        destination_object="{{ ti.xcom_pull(task_ids='check_month_process_file', key='output_filename') }}",
        dag=dag,
    )

    check_month_process_file >> move_equifax_inbound_to_outbound_bucket

    check_month_process_file >> move_file_to_processing_zone >> create_cluster >> submit_java_spark_job >> move_file_to_outbound_landing
