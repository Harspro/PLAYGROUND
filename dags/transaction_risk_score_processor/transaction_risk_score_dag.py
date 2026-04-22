import json
from datetime import timedelta
import logging
import pendulum
from google.cloud import storage, bigquery
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator
)
from typing import Final

import util.constants as consts
from util.miscutils import (
    read_variable_or_file,
    get_cluster_name,
    get_cluster_config_by_job_size,
    read_yamlfile_env_suffix,
)

# Load configurations
gcp_config = read_variable_or_file(consts.GCP_CONFIG)
deploy_env = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
deploy_env_suffix = gcp_config[consts.DEPLOY_ENV_STORAGE_SUFFIX]
dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)

# Set directory paths and load job configuration
config_dir = f'{settings.DAGS_FOLDER}/transaction_risk_score_processor/{consts.CONFIG}'
job_config = read_yamlfile_env_suffix(
    fname=f'{config_dir}/trs_processing_config.yaml',
    deploy_env=deploy_env,
    env_suffix=deploy_env_suffix
)

# Extract configuration values
dag_id = job_config[consts.DAG_ID]
ephemeral_cluster_name = get_cluster_name(True, dataproc_config, dag_id=dag_id)
dag_owner = job_config['dag_owner']
curated_project_id = gcp_config.get(consts.CURATED_ZONE_PROJECT_ID)
domain_scoring_dataset_id = job_config['business_dataset_id']
business_view = job_config['business_view']
bq_curated_table_trs_input = job_config['bq_curated_table_trs_input']
bq_dag_folder = job_config['bq_dag_folder']
pmml_file = job_config['pmml_file']

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


def build_pmml_spark_fetching_job() -> dict:
    """
    Build the configuration for the PMML Spark job.

    Returns:
        dict: Job configuration dictionary
    """
    dag_info = f"[dag_name: {dag_id}, dag_run_id: {'{{run_id}}'}, dag_owner: {dag_owner}]"
    logging.info('Building PMML Spark job configuration')

    return {
        consts.REFERENCE: {
            consts.PROJECT_ID: dataproc_config.get(consts.PROJECT_ID)
        },
        consts.PLACEMENT: {
            consts.CLUSTER_NAME: ephemeral_cluster_name
        },
        consts.SPARK_JOB: {
            consts.FILE_URIS: job_config[consts.FILE_URIS],
            consts.JAR_FILE_URIS: job_config[consts.JAR_FILE_URIS],
            consts.MAIN_CLASS: job_config[consts.MAIN_CLASS],
            consts.ARGS: [
                f'project={curated_project_id}',
                f'dataset={domain_scoring_dataset_id}',
                f'table={bq_curated_table_trs_input}',
                f'business.project={curated_project_id}',
                f'business.dataset={domain_scoring_dataset_id}',
                f'business.view={business_view}',
                f'bq.dag.folder={bq_dag_folder}',
                f'pmml.file={pmml_file}',
                f"{consts.SPARK_DAG_INFO}={dag_info}"
            ]
        }
    }


severity_tag = default_args['severity']
# DAG definition
with DAG(
        dag_id=dag_id,
        description='TRS calculator DAG',
        schedule='0 8 * * 1-5',
        default_args=default_args,
        tags=[severity_tag],
        catchup=False,
        is_paused_upon_creation=True,
        max_active_runs=1,
        start_date=pendulum.datetime(2023, 11, 27, tz='America/Toronto')
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

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

    trigger_jspark_pmml_job_task = DataprocSubmitJobOperator(
        task_id="trigger_jspark_pmml_job_task",
        job=build_pmml_spark_fetching_job(),
        region=dataproc_config.get(consts.LOCATION),
        project_id=dataproc_config.get(consts.PROJECT_ID),
        gcp_conn_id=gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID)
    )

    # Set task dependencies
    (start >> create_spark_cluster_task >> trigger_jspark_pmml_job_task >> end)
