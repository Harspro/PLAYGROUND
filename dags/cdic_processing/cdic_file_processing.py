from datetime import datetime, timedelta

import pendulum
import pytz
import logging
from typing import Final
import util.constants as consts
from airflow import DAG
from airflow import configuration as conf
from airflow import settings
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from google.cloud import bigquery
from util.miscutils import (
    read_variable_or_file,
    get_cluster_name,
    get_cluster_config_by_job_size,
    read_yamlfile_env_suffix,
    read_file_env,
    get_cluster_name_for_dag
)

from util.bq_utils import run_bq_query

logger = logging.getLogger(__name__)

gcp_config = read_variable_or_file(consts.GCP_CONFIG)
deploy_env = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
deploy_env_suffix = gcp_config['deploy_env_storage_suffix']
dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)
config_dir = f'{settings.DAGS_FOLDER}/cdic_processing/{consts.CONFIG}'

job_config = read_yamlfile_env_suffix(
    fname=f'{config_dir}/cdic_job_config.yaml',
    deploy_env=deploy_env, env_suffix=deploy_env_suffix)

file_list_config = read_yamlfile_env_suffix(
    fname=f'{config_dir}/cdic_file_list_config.yaml',
    deploy_env=deploy_env, env_suffix=deploy_env_suffix)

dag_id = job_config['dag_id']
dag_owner = job_config['dag_owner']
ephemeral_cluster_name = get_cluster_name_for_dag(dag_id)
curated_project_id = gcp_config.get('curated_zone_project_id')
landing_project_id = gcp_config.get('landing_zone_project_id')
processing_project_id = gcp_config.get('processing_zone_project_id')
control_table_dataset_id = job_config['control_table_dataset_id']
domain_dataset = job_config['domain_dataset']
cots_dataset = job_config['cots_dataset']
inbound_bucket = job_config['inbound_bucket']
staging_bucket = job_config['staging_bucket']
outbound_bucket = job_config['outbound_bucket']
control_table_name = job_config['control_table_name']
mask_flag = job_config['mask_flag']
landing_input_dir = "temenos_inbound_cdic"
processing_output_dir = "cdic_processing/output"
processing_input_dir = "cdic_processing/input"

CURR_DT: Final = datetime.now().astimezone(pytz.timezone("America/Toronto")).strftime("%Y%m%d%H%M%S")
FILE_EXTRACT_TYPE: Final = "2"
FILE_DATA_REQ_VER: Final = "310"
FILE_SUBSYSTEM_ID: Final = "003"
OUTPUT_FILE_PREFIX: Final = f"PCBA{CURR_DT}"

default_args = {
    'owner': 'team-defenders-alerts',
    'capability': 'risk-management',
    'severity': 'P3',
    'sub_capability': 'deposit-insurance-cdic',
    'business_impact': 'Compliance with CDIC',
    'customer_impact': 'None',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=5 * 60)
}


def enrich_data_to_final_table(sql_file_path):
    file_sql = read_file_env(
        fname=f'{config_dir}/sql_queries/{sql_file_path}',
        deploy_env=deploy_env)
    logger.info(f'file_sql : \n{file_sql}')
    run_query = run_bq_query(file_sql)
    query_result = run_query.result()
    logger.info(f"Result of query run: {query_result}")


def build_spark_fetching_job(file_type):
    file_name = f"{OUTPUT_FILE_PREFIX}{file_type[5:]}{FILE_EXTRACT_TYPE}{FILE_DATA_REQ_VER}{FILE_SUBSYSTEM_ID}.txt"
    dag_info = f"[dag_name: {dag_id}, dag_run_id: {'{{run_id}}'}, dag_owner: {dag_owner}]"
    args = [
        "config.filepath=application.yaml",
        "run.date= " + datetime.now().astimezone(pytz.timezone('America/Toronto')).strftime('%Y-%m-%d'),
        f"project= {curated_project_id}",
        f"dataset= {cots_dataset}",
        f"table= {file_list_config[file_type]['final_table']}",
        f"columns.to.mask= {file_list_config[file_type]['columns_to_mask']}",
        f"columns.to.hash= {file_list_config[file_type]['columns_to_hash']}",
        "output.path= " + f"gs://{staging_bucket}/{processing_output_dir}/{file_type}",
        f"filename= {file_name}",
        f"mask = {mask_flag}",
        f"{consts.SPARK_DAG_INFO}={dag_info}"
    ]

    if not file_list_config[file_type]['where_condition'] == "":
        args.append(f"where.condition= {file_list_config[file_type]['where_condition']}")

    return {
        consts.REFERENCE: {consts.PROJECT_ID: dataproc_config.get(consts.PROJECT_ID)},
        consts.PLACEMENT: {consts.CLUSTER_NAME: ephemeral_cluster_name},
        consts.SPARK_JOB: {
            consts.JAR_FILE_URIS: job_config[consts.JAR_FILE_URIS],
            consts.MAIN_CLASS: job_config[consts.MAIN_CLASS],
            consts.ARGS: args
        }
    }


severityTag = default_args['severity']


with (DAG(
        dag_id=dag_id,
        description='CDIC File Processing DAG',
        schedule=None,
        default_args=default_args,
        tags=[severityTag],
        catchup=False,
        is_paused_upon_creation=True,
        max_active_runs=1,
        max_active_tasks=16,
        start_date=pendulum.today('America/Toronto').add(days=-1)
) as dag):
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    file_specific_tasks = []
    enrichment_tasks = []
    spark_submit_and_move_file_to_landing_tasks = []

    for file_type in file_list_config:

        with TaskGroup(group_id=f"{file_type}_initial_process") as initial_processing_task_group:
            wait_for_file_with_prefix = GCSObjectsWithPrefixExistenceSensor(
                task_id=f"wait_for_{file_type}",
                bucket=inbound_bucket,
                prefix=f"{landing_input_dir}/temenos_pcb_edsa_{file_type}",
                poke_interval=3 * 60,
                timeout=12 * 60 * 60,
            )

            move_file_to_process = GCSToGCSOperator(
                task_id=f"move_file_to_processing_{file_type}",
                gcp_conn_id=gcp_config.get(consts.LANDING_ZONE_CONNECTION_ID),
                source_bucket=inbound_bucket,
                source_object=f"{landing_input_dir}/temenos_pcb_edsa_{file_type}",
                destination_bucket=staging_bucket,
                destination_object=f"{processing_input_dir}/",
                match_glob="**/*",
                move_object=True,
                dag=dag
            )

            if file_list_config[file_type].get('csv_to_bq_config'):
                sourceUris = file_list_config[file_type]['csv_to_bq_config']['load'].get('sourceUris')
                if sourceUris.startswith('{composer_bucket}'):
                    file_path = sourceUris.split("{composer_bucket}/")[1]
                    source_bucket = conf.get('logging', 'remote_base_log_folder')
                    if source_bucket.endswith('/logs'):
                        source_bucket = source_bucket[:-5]
                        source_file_path = f"{source_bucket}/{file_path}"
                        file_list_config[file_type]['csv_to_bq_config']['load']['sourceUris'] = source_file_path
                else:
                    source_file_path = sourceUris

                insert_data_to_BQ = BigQueryInsertJobOperator(
                    task_id=f'insert_csv_to_BQ_{file_type}',
                    configuration=file_list_config[file_type]['csv_to_bq_config'],
                    location=gcp_config.get(consts.BQ_QUERY_LOCATION)
                )
                wait_for_file_with_prefix >> move_file_to_process >> insert_data_to_BQ
            else:
                wait_for_file_with_prefix >> move_file_to_process

            file_specific_tasks.append(initial_processing_task_group)

        if file_list_config[file_type]['enrichment_flag']:
            sql_file_path = file_list_config[file_type]['sql_file']
            enrich_data_task = PythonOperator(
                task_id=f"enrich_data_to_final_table_{file_type}",
                python_callable=enrich_data_to_final_table,
                op_args=[sql_file_path]
            )
            enrichment_tasks.append(enrich_data_task)

        with TaskGroup(group_id=f"{file_type}_file_creation_and_move_to_landing") as file_creation_and_move_group:

            if file_list_config[file_type]['processing_flag']:
                file_creation_spark_job = DataprocSubmitJobOperator(
                    task_id=f"submit_spark_job_{file_type}",
                    job=build_spark_fetching_job(file_type),
                    region=dataproc_config.get(consts.LOCATION),
                    project_id=dataproc_config.get(consts.PROJECT_ID),
                    gcp_conn_id=gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID),
                )
                source_object_prefix = processing_output_dir + "/" + file_type
                source_object_suffix = "PCBA"
                logger.info(f"Processing Bucket is: {source_object_prefix}")

            else:
                source_object_prefix = processing_input_dir
                source_object_suffix = f"temenos_pcb_edsa_{file_type}"
                logger.info(f"Inbound Bucket is: {source_object_prefix}")

            dest_obj = f"{OUTPUT_FILE_PREFIX}{file_type[5:]}{FILE_EXTRACT_TYPE}{FILE_DATA_REQ_VER}{FILE_SUBSYSTEM_ID}.txt"

            # PCBA202310230932270100.csv

            move_file_to_outbound = GCSToGCSOperator(
                task_id=f'move_file_to_landing_{file_type}',
                gcp_conn_id=gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID),
                source_bucket=staging_bucket,
                source_object=f"{source_object_prefix}/{source_object_suffix}",
                destination_bucket=outbound_bucket,
                destination_object=dest_obj,
                match_glob="**/*",
                dag=dag,
            )

            archive_file = GCSToGCSOperator(
                task_id=f'archive_file_{file_type}',
                gcp_conn_id=gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID),
                source_bucket=staging_bucket,
                source_object=f"{processing_input_dir}/temenos_pcb_edsa_{file_type}",
                destination_bucket=inbound_bucket,
                destination_object=f"{landing_input_dir}/archive/{file_type}/",
                match_glob="**/*",
                move_object=True,
                dag=dag,
            )

            if file_list_config[file_type]['processing_flag']:
                file_creation_spark_job >> move_file_to_outbound >> archive_file
            else:
                move_file_to_outbound >> archive_file
            spark_submit_and_move_file_to_landing_tasks.append(file_creation_and_move_group)

    create_cluster = DataprocCreateClusterOperator(
        task_id=consts.CLUSTER_CREATING_TASK_ID,
        project_id=dataproc_config.get(consts.PROJECT_ID),
        cluster_config=get_cluster_config_by_job_size(deploy_env, gcp_config.get(consts.NETWORK_TAG), "medium"),
        region=dataproc_config.get(consts.LOCATION),
        cluster_name=ephemeral_cluster_name,
    )

    start >> file_specific_tasks
    for task_group in file_specific_tasks:
        task_group >> enrichment_tasks
    enrichment_tasks >> create_cluster >> spark_submit_and_move_file_to_landing_tasks
    spark_submit_and_move_file_to_landing_tasks >> end
