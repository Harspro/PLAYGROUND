import logging
import os
import re
from datetime import timedelta, datetime
from typing import Final

import pendulum
from airflow import DAG, settings
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import util.constants as consts
from util.deploy_utils import read_pause_unpause_setting, pause_unpause_dag
from util.logging_utils import build_spark_logging_info
from util.miscutils import read_file_env, read_variable_or_file, read_yamlfile_env, get_cluster_config_by_job_size

from util.bq_utils import run_bq_query
from tokenized_consent_capture.external_task_status_pokesensor import ExternalTaskPokeSensor

logger = logging.getLogger(__name__)

TOKENIZED_CONSCENT_CAPTURE_CONFIG_FILE: Final = 'tokenized_consent_kafka_writer_config.yaml'

gcp_config = read_variable_or_file(consts.GCP_CONFIG)
deploy_env_name = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
tokenized_consent_capture_config = read_yamlfile_env(f'{settings.DAGS_FOLDER}/tokenized_consent_capture/'
                                                     f'{TOKENIZED_CONSCENT_CAPTURE_CONFIG_FILE}', deploy_env_name)

local_tz = pendulum.timezone('America/Toronto')
dag_config = tokenized_consent_capture_config.get(consts.DAG)
DAG_ID: Final = dag_config.get(consts.DAG_ID)
schedule = dag_config.get(consts.SCHEDULE_INTERVAL)

DAG_DEFAULT_ARGS = {
    'owner': 'team-ogres-alerts',
    'capability': 'legal-compliance',
    'severity': 'P2',
    'sub_capability': 'consent',
    'business_impact': 'This will impact the compliance',
    'customer_impact': 'none',
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retry_delay': timedelta(seconds=10)
}


def validate_record_count(**kwargs):
    ti = kwargs['ti']
    record_count = ti.xcom_pull(task_ids=consts.QUERY_BIGQUERY_TABLE_TASK_ID, key='record_count')
    if record_count > 0:
        return consts.KAFKA_WRITER_TASK
    else:
        return consts.END_TASK_ID


def get_file_date(file_path: str):
    file_name = os.path.basename(file_path)
    date_pattern = r"(\d{4})(\d{2})(\d{2})"
    date_str = re.search(date_pattern, file_name).group()
    derived_date = datetime.strptime(date_str, "%Y%m%d").date()
    return derived_date


def run_bigquery_staging_query(query: str, project_id: str, dataset_id: str, table_id: str, file_name: str, **kwargs):
    """
    Runs bigquery staging query to create staging table.

    :param query: Staging Query.
    :type query: str

    :param project_id: BigQuery project ID.
    :type project_id: str

    :param dataset_id: BigQuery dataset ID.
    :type dataset_id: str

    :param table_id: BigQuery table ID.
    :type table_id: str

    :param file_name: data file.
    :type file_name: str
    """
    logger.info("Running query provided to create staging table in GCP processing zone.")
    logger.info("Extract date from the file name")
    file_create_date = get_file_date(file_name) if file_name else None
    file_create_date_filter = f"M.FILE_CREATE_DT = '{str(file_create_date)}'"
    ext_filter = kwargs["dag_run"].conf.get("file_create_date_filter")
    staging_table_id = f"{project_id}.{dataset_id}.{table_id}"
    if ext_filter:
        file_create_date_filter = f"M.{ext_filter}"
    stg_query = query.replace(f"{{{consts.STAGING_TABLE_ID}}}", staging_table_id).replace(
        f"{{{'file_create_date_filter'}}}", file_create_date_filter)
    logger.info(f"Running stg query: {stg_query}")
    run_bq_query(stg_query)
    bq_data_check_query = f"""SELECT COUNT(*) AS RECORDS_COUNT FROM`{staging_table_id}`"""
    bq_data_check_results = run_bq_query(bq_data_check_query).result()
    rec_count = next(bq_data_check_results).get('RECORDS_COUNT')
    logger.info(f"Staging table record counts: {rec_count}")
    ti = kwargs['ti']
    ti.xcom_push(key='record_count', value=rec_count)
    ti.xcom_push(key='file_create_date', value=file_create_date)


with DAG(dag_id=DAG_ID,
         default_args=DAG_DEFAULT_ARGS,
         schedule=schedule,
         description='DAG to send Tokenized Consent data read data from BigQuery and send to DP Kafka',
         render_template_as_native_obj=True,
         start_date=datetime(2023, 1, 1, tzinfo=local_tz),
         max_active_runs=1,
         catchup=False,
         dagrun_timeout=timedelta(hours=24),
         is_paused_upon_creation=True,
         params={"file_create_date_filter": "", "name": None, "skip_sensor_task": False}
         ) as dag:
    if tokenized_consent_capture_config[consts.READ_PAUSE_DEPLOY_CONFIG]:
        is_paused = read_pause_unpause_setting(DAG_ID, deploy_env_name)
        pause_unpause_dag(dag, is_paused)

    dest_project = gcp_config.get(consts.LANDING_ZONE_PROJECT_ID)
    bq_config = tokenized_consent_capture_config.get(consts.BIGQUERY)
    gcs_config = tokenized_consent_capture_config.get(consts.GCS)
    bq_stg_project_id = bq_config.get(consts.PROJECT_ID)
    bq_stg_dataset = bq_config.get(consts.DATASET_ID)
    bq_stg_table_id = bq_config.get(consts.TABLE_ID).replace(
        consts.CURRENT_DATE_PLACEHOLDER,
        datetime.now(tz=local_tz).strftime('%Y_%m_%d')
    )
    if bq_config.get(consts.QUERY):
        query = bq_config.get(consts.QUERY)
    elif bq_config.get(consts.QUERY_FILE):
        query_sql_file = f"{settings.DAGS_FOLDER}/" \
                         f"{bq_config.get(consts.QUERY_FILE)}"
        query = read_file_env(query_sql_file, deploy_env_name)
    else:
        raise AirflowFailException("Please provide query, none found.")

    start_point = EmptyOperator(task_id=consts.START_TASK_ID)
    end_point = EmptyOperator(task_id=consts.END_TASK_ID)

    bigquery_stg_query_task = PythonOperator(
        task_id=consts.QUERY_BIGQUERY_TABLE_TASK_ID,
        python_callable=run_bigquery_staging_query,
        op_kwargs={consts.QUERY: query,
                   consts.PROJECT_ID: bq_stg_project_id,
                   consts.DATASET_ID: bq_stg_dataset,
                   consts.TABLE_ID: bq_stg_table_id,
                   consts.FILE_NAME: "{{ dag_run.conf['name'] }}"
                   },
        retries=2
    )

    kafka_writer_task = TriggerDagRunOperator(
        task_id=consts.KAFKA_WRITER_TASK,
        trigger_dag_id=tokenized_consent_capture_config.get(consts.KAFKA_TRIGGER_DAG_ID),
        logical_date=datetime.now(local_tz),
        conf={
            consts.BUCKET: gcs_config.get(consts.BUCKET),
            consts.NAME: gcs_config.get(consts.BUCKET),
            consts.FOLDER_NAME: gcs_config.get(consts.FOLDER_NAME),
            consts.FILE_NAME: gcs_config.get(consts.FILE_NAME),
            consts.CLUSTER_NAME: tokenized_consent_capture_config.get(consts.KAFKA_CLUSTER_NAME)
        },
        wait_for_completion=True
    )

    bq_record_count_check = BranchPythonOperator(
        task_id='bq_record_count_check',
        python_callable=validate_record_count,
        retries=2
    )

    monitor_source_data_dag = ExternalTaskPokeSensor(
        task_id=f'monitor_{dag_config.get(consts.EXTERNAL_SENSOR_DAG_ID)}',
        external_dag_id=dag_config.get(consts.EXTERNAL_SENSOR_DAG_ID),
        external_task_id='End',
        start_date_delta=timedelta(minutes=-10),
        end_date_delta=timedelta(minutes=180),
        mode='poke',
        poke_interval=60 * dag_config.get(consts.POKE_INTERVAL_MIN),
        timeout=60 * dag_config.get(consts.POKE_TIMEOUT_MIN),
        dag=dag
    )
    start_point >> monitor_source_data_dag >> bigquery_stg_query_task >> bq_record_count_check >> kafka_writer_task >> end_point
    bq_record_count_check >> end_point
