import json
from datetime import datetime, timedelta
from typing import Final

import pendulum
import pytz
import util.constants as consts
from airflow import DAG
from airflow import settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from google.cloud import bigquery
from util.miscutils import (
    read_variable_or_file,
    get_cluster_name,
    get_cluster_config_by_job_size,
    read_yamlfile_env_suffix,
    read_file_env
)
from util.gcs_utils import (
    read_file,
    write_file,
    gcs_file_exists
)

gcp_config = read_variable_or_file(consts.GCP_CONFIG)
deploy_env = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
deploy_env_suffix = gcp_config['deploy_env_storage_suffix']
dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)
config_dir = f'{settings.DAGS_FOLDER}/risk_adaptive_file_processing/{consts.CONFIG}'

job_config = read_yamlfile_env_suffix(
    fname=f'{config_dir}/risk_adaptive_file_config.yaml',
    deploy_env=deploy_env, env_suffix=deploy_env_suffix)
create_table_query = read_file_env(
    fname=f'{config_dir}/create_table_query.sql',
    deploy_env=deploy_env)

dag_id = job_config['dag_id']
ephemeral_cluster_name = get_cluster_name(True, dataproc_config, dag_id=dag_id)
dag_owner = job_config['dag_owner']
curated_project_id = gcp_config.get('curated_zone_project_id')
landing_project_id = gcp_config.get('landing_zone_project_id')
processing_project_id = gcp_config.get('processing_zone_project_id')
control_table_dataset_id = job_config['control_table_dataset_id']
control_table_name = job_config['control_table_name']
sc_job_name = job_config['sc_job_name']
am_job_name = job_config['am_job_name']
staging_bucket = job_config['staging_bucket']
output_bucket = job_config['output_bucket']
segment_dataset_id = job_config['segment_dataset_id']
am_table_name = job_config['am_table_name']
sc_table_name = job_config['sc_table_name']
ah_table_name = job_config['ah_table_name']
FILE_ID_FILEPATH: Final = "equifax_processing/file_id/file_id.txt"
PROCESSING_INTERIM_DIR: Final = "equifax_processing/equifax_score_gen/interim/"
PROCESSING_OUTPUT_DIR: Final = "equifax_processing/equifax_score_gen/output/"
AM_FILE_CREATE_DATE_PLACEHOLDER: Final = '{am_file_create_date}'
SC_FILE_CREATE_DATE_PLACEHOLDER: Final = '{sc_file_create_date}'
AH_FILE_CREATE_DATE_PLACEHOLDER: Final = '{ah_file_create_date}'
FILE_ID_PLACEHOLDER: Final = '{file_id}'
eq_file_prefix = 'pd' if deploy_env == "prod" else 'tt'

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


def check_am_sc_control_table():
    sc_query_statement = f"""
                    SELECT MAX(load_timestamp) AS load_timestamp
                    FROM `{curated_project_id}.{control_table_dataset_id}.{control_table_name}`
                    WHERE  dag_id = "{sc_job_name}"
                    """
    am_query_statement = f"""
                    SELECT MAX(load_timestamp) AS load_timestamp
                    FROM `{curated_project_id}.{control_table_dataset_id}.{control_table_name}`
                    WHERE  dag_id = "{am_job_name}"
                    """
    client = bigquery.Client()
    sc_query_result = client.query(sc_query_statement).result().to_dataframe()
    am_query_result = client.query(am_query_statement).result().to_dataframe()
    max_time_am = am_query_result['load_timestamp'].iloc[0].astimezone(pytz.timezone('America/Toronto'))
    max_time_sc = sc_query_result['load_timestamp'].iloc[0].astimezone(pytz.timezone('America/Toronto'))
    current_time = datetime.now().astimezone(pytz.timezone('America/Toronto'))
    if max_time_sc < (current_time - timedelta(days=10)) or max_time_am < (current_time - timedelta(days=10)):
        raise Exception("AM and SC are not updated yet.")
    return 'fetch_file_id'


def fetch_file_id(**context):
    if gcs_file_exists(staging_bucket, FILE_ID_FILEPATH):
        # read file and fetch current file_id
        json_data = read_file(staging_bucket, FILE_ID_FILEPATH)
    else:
        # if file doesn't exist then write new one with pre-defined value.
        json_data = """{
            "file_id_value": 117,
            "usage": 0,
            "last_used": "Never"
        }"""
        write_file(staging_bucket, FILE_ID_FILEPATH, json_data)
    file_id = (json.loads(json_data))["file_id_value"]
    file_name = "eqxds_expcbanktsys" + eq_file_prefix + "_SCRQ_" + str(file_id).zfill(4) + "_" + \
                datetime.now().astimezone(pytz.timezone('America/Toronto')).strftime('%Y%m%d%H%M%S%f')[:16] + ".txt"
    context['task_instance'].xcom_push(key='file_name', value=file_name)
    context['task_instance'].xcom_push(key='file_id', value=file_id)


def increment_file_id():
    if gcs_file_exists(staging_bucket, FILE_ID_FILEPATH):
        # read file and fetch current file_id
        json_data = json.loads(read_file(staging_bucket, FILE_ID_FILEPATH))
        file_id = json_data['file_id_value']
        usage = json_data['usage']

        # update the file_id, usage and last_used
        file_id_incremented = f"""{{
            "file_id_value": {file_id + 1},
            "usage": {usage + 1},
            "last_used": "{datetime.now().astimezone(pytz.timezone('America/Toronto'))}"
        }}"""
        write_file(staging_bucket, FILE_ID_FILEPATH, file_id_incremented)
    else:
        raise Exception(f"{FILE_ID_FILEPATH} does not exist.")


def create_table(**context):
    file_id = context['task_instance'].xcom_pull(task_ids='fetch_file_id', key='file_id')
    am_segment_sql = f"""
                    SELECT FILE_CREATE_DT
                    FROM `{curated_project_id}.{segment_dataset_id}.{am_table_name}`
                    WHERE FILE_CREATE_DT >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)
                    ORDER BY FILE_CREATE_DT DESC
                    LIMIT 1
                    """
    sc_segment_sql = f"""
                    SELECT FILE_CREATE_DT
                    FROM `{curated_project_id}.{segment_dataset_id}.{sc_table_name}`
                    WHERE FILE_CREATE_DT >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)
                    ORDER BY FILE_CREATE_DT DESC
                    LIMIT 1
                    """
    ah_segment_sql = f"""
                    SELECT MAX(FILE_CREATE_DT) AS FILE_CREATE_DT
                    FROM `{curated_project_id}.{segment_dataset_id}.{ah_table_name}`
                    WHERE FILE_CREATE_DT BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH) AND CURRENT_DATE()
                    AND (
                        (EXTRACT(MONTH FROM CURRENT_DATE()) = 1 AND EXTRACT(MONTH FROM FILE_CREATE_DT) = 12)
                        OR
                        (EXTRACT(MONTH FROM FILE_CREATE_DT) = EXTRACT(MONTH FROM CURRENT_DATE()) - 1)
                    )
                    """
    client = bigquery.Client()
    am_sql_result = client.query(am_segment_sql).result().to_dataframe()
    sc_sql_result = client.query(sc_segment_sql).result().to_dataframe()
    ah_sql_result = client.query(ah_segment_sql).result().to_dataframe()
    am_file_create_dt = am_sql_result['FILE_CREATE_DT'].iloc[0].strftime('%Y-%m-%d')
    sc_file_create_dt = sc_sql_result['FILE_CREATE_DT'].iloc[0].strftime('%Y-%m-%d')
    ah_file_create_dt = ah_sql_result['FILE_CREATE_DT'].iloc[0].strftime('%Y-%m-%d')
    create_table_job = client.query(create_table_query.replace(AM_FILE_CREATE_DATE_PLACEHOLDER, am_file_create_dt)
                                                      .replace(SC_FILE_CREATE_DATE_PLACEHOLDER, sc_file_create_dt)
                                                      .replace(AH_FILE_CREATE_DATE_PLACEHOLDER, ah_file_create_dt)
                                                      .replace(FILE_ID_PLACEHOLDER, "SCRQ" + str(file_id).zfill(4)))
    create_table_job.result()


def build_spark_fetching_job():
    dag_info = f"[dag_name: {dag_id}, dag_run_id: {'{{run_id}}'}, dag_owner: {dag_owner}]"
    return {
        consts.REFERENCE: {consts.PROJECT_ID: dataproc_config.get(consts.PROJECT_ID)},
        consts.PLACEMENT: {consts.CLUSTER_NAME: ephemeral_cluster_name},
        consts.SPARK_JOB: {
            consts.JAR_FILE_URIS: job_config[consts.JAR_FILE_URIS],
            consts.MAIN_CLASS: job_config[consts.MAIN_CLASS],
            consts.ARGS: [
                "config.filepath=application.yaml",
                "run.date= " + datetime.now().astimezone(pytz.timezone('America/Toronto')).strftime('%Y-%m-%d'),
                "file.id= {{ ti.xcom_pull(task_ids='fetch_file_id', key='file_id') }}",
                "file.name= {{ ti.xcom_pull(task_ids='fetch_file_id', key='file_name') }}",
                f'interim.filepath= gs://{staging_bucket}/{PROCESSING_INTERIM_DIR}',
                f'output.filepath= gs://{staging_bucket}/{PROCESSING_OUTPUT_DIR}',
                f'project= {processing_project_id}',
                f'{consts.SPARK_DAG_INFO}={dag_info}'
            ]
        }
    }


severity_tag = default_args['severity']

with DAG(
        dag_id=dag_id,
        description='Risk Adapative File Processing DAG',
        schedule="00 08 2 * *",
        default_args=default_args,
        tags=[severity_tag],
        catchup=False,
        is_paused_upon_creation=True,
        max_active_runs=1,
        start_date=pendulum.datetime(2023, 11, 27, tz='America/Toronto')
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    check_latest_run_am_sc = BranchPythonOperator(
        task_id='check_latest_run_am_sc',
        python_callable=check_am_sc_control_table,
        trigger_rule='one_success',
    )

    fetch_file_id = PythonOperator(
        task_id="fetch_file_id",
        python_callable=fetch_file_id
    )

    create_final_table = PythonOperator(
        task_id="create_final_table",
        python_callable=create_table
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id=consts.CLUSTER_CREATING_TASK_ID,
        project_id=dataproc_config.get(consts.PROJECT_ID),
        cluster_config=get_cluster_config_by_job_size(deploy_env, gcp_config.get(consts.NETWORK_TAG), "small"),
        region=dataproc_config.get(consts.LOCATION),
        cluster_name=ephemeral_cluster_name,
    )

    file_creation_spark_job = DataprocSubmitJobOperator(
        task_id="submit_java_spark_job",
        job=build_spark_fetching_job(),
        region=dataproc_config.get(consts.LOCATION),
        project_id=dataproc_config.get(consts.PROJECT_ID),
        gcp_conn_id=gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID),
    )

    output_bucket = GCSToGCSOperator(
        task_id='move_file_to_landing',
        gcp_conn_id=gcp_config.get("processing_zone_connection_id"),
        source_bucket=staging_bucket,
        source_object=f"{PROCESSING_INTERIM_DIR}{{{{ ti.xcom_pull(task_ids='fetch_file_id', key='file_name') }}}}",
        destination_bucket=output_bucket,
        destination_object="{{ ti.xcom_pull(task_ids='fetch_file_id', key='file_name') }}",
        dag=dag,
    )

    increment_file_id = PythonOperator(
        task_id="increment_file_id",
        python_callable=increment_file_id
    )

    (start >> check_latest_run_am_sc >> fetch_file_id >> create_final_table >> create_cluster >> file_creation_spark_job >> output_bucket >> increment_file_id >> end)
