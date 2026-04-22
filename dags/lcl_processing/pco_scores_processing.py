import json
import logging
import os
import uuid
import pprint

from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import (DataprocSubmitJobOperator, DataprocCreateClusterOperator)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

from datetime import datetime, timedelta
from google.cloud import bigquery

from util.miscutils import (read_variable_or_file, get_cluster_name, get_ephemeral_cluster_config)

# These paths are relative to the main dags folder
PCO_CONFIG_FILE = 'lcl_processing/pco_scores_config'
SCHEMA_FILE = 'lcl_processing/pco_scores_table_schema.json'

gcp_config = read_variable_or_file("gcp_config")
deploy_env_name = gcp_config['deployment_environment_name']
deploy_env_suffix = gcp_config['deploy_env_storage_suffix']

dataproc_config = read_variable_or_file("dataproc_config")

# print("Reading pco scores config")
pco_scores_config = read_variable_or_file(PCO_CONFIG_FILE, deploy_env_name)

is_ephemeral = True
DAG_ID = "PCO_Scores"
CLUSTER_NAME = get_cluster_name(is_ephemeral, dataproc_config, dag_id=DAG_ID)

DAG_DEFAULT_ARGS = {
    "owner": "team-convergence-alerts",
    'capability': 'Loyalty',
    'severity': 'P3',
    'sub_capability': 'Loyalty',
    'business_impact': 'Automatic Approval rating for NHNS customer will drop and more applications will be pushed to '
                       'manual review.',
    'customer_impact': 'Percentage of Instant approval for customers with less data around their credit risk profile '
                       'will drop.',
    "start_date": datetime(2021, 1, 1),
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "max_active_runs": 10,
    "retries": 0,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True
}


# For filepaths, always start from the dags folder
def read_schema(schema_filename: str) -> list:
    print(f'curdir: {os.getcwd()}')

    dagsdir = settings.DAGS_FOLDER
    schema_filepath = f"{dagsdir}/{schema_filename}"
    if os.path.exists(schema_filepath):
        with open(schema_filepath, 'r') as schema_file:
            content = schema_file.read()
            schema_list = json.loads(content)
    else:
        print(f'{schema_filepath} doesnt exist')

    return schema_list


def load_file_to_bq_external(**context):
    filename = context['dag_run'].conf.get('name')
    staging_bucket = pco_scores_config['staging_bucket']

    source_file_path = f"gs://{staging_bucket}/{filename}"

    dest_project = f"{gcp_config.get('processing_zone_project_id')}"
    dest_dataset = pco_scores_config['staging_bq_dataset']
    dest_table = pco_scores_config['staging_external_bq_table']

    bq_table_id = f"{dest_project}.{dest_dataset}.{dest_table}"

    bq_table_schema = [bigquery.SchemaField.from_api_repr(s) for s in read_schema(SCHEMA_FILE)]

    client = bigquery.Client()
    client.delete_table(bq_table_id, not_found_ok=True)

    external_config_api = {
        "autodetect": False,
        "csvOptions": {
            "skipLeadingRows": 1,
            "fieldDelimiter": '|'
        },
        "sourceFormat": "CSV",
        "sourceUris": [source_file_path]
    }
    external_config = bigquery.ExternalConfig.from_api_repr(external_config_api)

    external_table = bigquery.Table(bq_table_id, schema=bq_table_schema)
    external_table.external_data_configuration = external_config

    client.create_table(external_table)

    target_table = client.get_table(bq_table_id)
    logging.info(f"Loaded {source_file_path} to BQ table {bq_table_id}, which now has {target_table.num_rows} rows")


def create_bq_staging(**context):
    uuid_str = str(uuid.uuid4())
    source_filename = os.path.basename(context['dag_run'].conf.get('name'))
    print(f"source_filename = {source_filename}")

    context['ti'].xcom_push(key='execution_id', value=uuid_str)
    context['ti'].xcom_push(key='source_filename', value=source_filename)
    context['ti'].xcom_push(key='is_valid', value=False)

    dest_project = f"{gcp_config.get('processing_zone_project_id')}"
    dest_dataset = pco_scores_config['staging_bq_dataset']

    source_table = pco_scores_config['staging_external_bq_table']
    dest_table = pco_scores_config['staging_bq_table']

    external_table_id = f"{dest_project}.{dest_dataset}.{source_table}"
    staging_table_id = f"{dest_project}.{dest_dataset}.{dest_table}"

    client = bigquery.Client()
    query = f'''
        DECLARE TIMESTAMP_CONST TIMESTAMP;

        SET TIMESTAMP_CONST = CURRENT_TIMESTAMP();

        CREATE OR REPLACE TABLE
          `{staging_table_id}`
        OPTIONS (
          expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE),
          description = "Staging temp table for PCO Scores",
          labels = [("name", "pco-scores")]
        ) AS
        SELECT
          t.*,
          "{uuid_str}" AS execution_id,
          TIMESTAMP_CONST AS load_timestamp,
          "{source_filename}" AS source_filename
        FROM
          `{external_table_id}` t
    '''
    query_job = client.query(query)
    query_job.result()
    context['ti'].xcom_push(key='is_valid', value=True)
    new_table = client.get_table(staging_table_id)
    new_schema = new_table.schema[:]
    print(f"bq staging table: new schema len = {len(new_schema)}")


def build_cluster_task(cluster_name: str, taskid: str):
    return DataprocCreateClusterOperator(
        task_id=taskid,
        project_id=gcp_config.get("processing_zone_project_id"),
        cluster_config=get_ephemeral_cluster_config(deploy_env_name, gcp_config.get("network_tag")),
        region="northamerica-northeast1",
        cluster_name=cluster_name
    )


def build_spark_job(**context):
    job_tmpl = {
        'reference': {'project_id': dataproc_config.get('project_id')},
        'placement': {'cluster_name': CLUSTER_NAME},
        'spark_job': pco_scores_config["ods_audit_spark_job"].copy()
    }
    source_filename = context['ti'].xcom_pull(task_ids='create_bq_staging_table', key='source_filename')
    execution_id = context['ti'].xcom_pull(task_ids='create_bq_staging_table', key='execution_id')

    job_tmpl['spark_job']['args']['pcb.pco.processor.source.filename'] = source_filename
    job_tmpl['spark_job']['args']['pcb.pco.processor.execution.id'] = execution_id

    arglist = []
    for k, v in job_tmpl['spark_job']['args'].items():
        arglist.append(f'{k}={v}')
    job_tmpl['spark_job']['args'] = arglist

    logging.info('\n**** spark_job_tmpl:')
    logging.info(pprint.pprint(job_tmpl))

    context['ti'].xcom_push(key='spark_job', value=job_tmpl)


def build_ods_audit_spark_job(taskid: str):
    return DataprocSubmitJobOperator(
        task_id=taskid,
        job="{{ ti.xcom_pull(task_ids='build_PCO_spark_job', key='spark_job') }}",
        region=dataproc_config.get('location'),
        project_id=dataproc_config.get('project_id'),
        gcp_conn_id=gcp_config.get('processing_zone_connection_id')
    )


dag = DAG(
    dag_id=DAG_ID,
    schedule=None,
    description="DAG to load the PCO scores data from Cloud Storage to BigQuery",
    render_template_as_native_obj=True,
    is_paused_upon_creation=True,
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)

with dag:
    start = EmptyOperator(task_id="Start")
    end = EmptyOperator(task_id="End")

    copy_from_landing_to_processing = GCSToGCSOperator(
        task_id="copy_pco_file_to_processing_bucket",
        gcp_conn_id=gcp_config.get("landing_zone_connection_id"),
        source_bucket="{{ dag_run.conf['bucket'] }}",
        source_object="{{ dag_run.conf['name'] }}",
        destination_bucket=pco_scores_config.get("staging_bucket"),
        destination_object="{{ dag_run.conf['name']}}",
    )

    load_bq_external_table = PythonOperator(
        task_id="load_file_to_bq_external_table",
        python_callable=load_file_to_bq_external,
    )

    staging_bq_table_ref = (f"{gcp_config.get('processing_zone_project_id')}."
                            f"{pco_scores_config.get('staging_bq_dataset')}."
                            f"{pco_scores_config.get('staging_bq_table')}")
    curated_bq_table_ref = (f"{gcp_config.get('curated_zone_project_id')}."
                            f"{pco_scores_config.get('curated_bq_dataset')}."
                            f"{pco_scores_config.get('curated_bq_table')}")

    create_PCO_bq_staging_table = PythonOperator(
        task_id="create_bq_staging_table",
        python_callable=create_bq_staging
    )

    build_PCO_spark_job = PythonOperator(
        task_id="build_PCO_spark_job",
        python_callable=build_spark_job
    )

    INSERT_INTO_CURATED_TABLE = f"""
        INSERT INTO `{curated_bq_table_ref}`
        SELECT t.* FROM `{staging_bq_table_ref}` t
    """
    insert_into_bq_curated = BigQueryInsertJobOperator(
        task_id="insert_into_bq_curated_table",
        configuration={
            "query": {
                "query": INSERT_INTO_CURATED_TABLE,
                "useLegacySql": False,
            }
        },
        location=gcp_config.get('bq_query_location')
    )

    PRUNE_CURATED_TABLE = f"""
        DELETE `{curated_bq_table_ref}`
        WHERE load_timestamp < TIMESTAMP_SUB(CURRENT_TIMESTAMP() , INTERVAL 90 DAY)
    """
    prune_bq_curated = BigQueryInsertJobOperator(
        task_id="prune_old_data_from_bq_curated",
        configuration={
            "query": {
                "query": PRUNE_CURATED_TABLE,
                "useLegacySql": False,
            }
        },
        location=gcp_config.get('bq_query_location')
    )

    create_cluster_task = build_cluster_task(CLUSTER_NAME, 'create_cluster')
    ods_audit_job = build_ods_audit_spark_job('create_ods_audit')

    start >> copy_from_landing_to_processing >> load_bq_external_table >> create_PCO_bq_staging_table \
        >> insert_into_bq_curated >> create_cluster_task >> build_PCO_spark_job >> ods_audit_job \
        >> prune_bq_curated >> end
