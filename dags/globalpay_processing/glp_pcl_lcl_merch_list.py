import pendulum
import util.constants as consts
import logging

from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

from typing import Final

from util.deploy_utils import read_pause_unpause_setting, pause_unpause_dag
from util.miscutils import read_variable_or_file, read_yamlfile_env
from util.bq_utils import check_bq_table_exists, run_bq_query

logger = logging.getLogger(__name__)

GLOBAL_PAY_MERCH_LIST_CONFIG_FILE: Final = 'glp_pcl_lcl_merch_list_config.yaml'
GLOBAL_PAYMENT_MERCHANT_SCHEMA: Final = 'global_payment_merchant_schema'
SQL_FOLDER: Final = 'sql_files'
DAG_ID: Final = 'glp_pcl_lcl_merch_list'

local_tz = pendulum.timezone('America/Toronto')

gcp_config = read_variable_or_file(consts.GCP_CONFIG)
deploy_env_name = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
glp_pcl_lcl_merch_list_config = read_yamlfile_env(f'{settings.DAGS_FOLDER}/globalpay_processing/'
                                                  f'{GLOBAL_PAY_MERCH_LIST_CONFIG_FILE}', deploy_env_name)
create_table_query = read_yamlfile_env(f'{settings.DAGS_FOLDER}/globalpay_processing/'
                                       f'{SQL_FOLDER}/create_table.sql', deploy_env_name)
new_store_query = read_yamlfile_env(f'{settings.DAGS_FOLDER}/globalpay_processing/'
                                    f'{SQL_FOLDER}/new_store.sql', deploy_env_name)
deactivate_store_query = read_yamlfile_env(f'{settings.DAGS_FOLDER}/globalpay_processing/'
                                           f'{SQL_FOLDER}/deactivate_store.sql', deploy_env_name)
reopen_store_query = read_yamlfile_env(f'{settings.DAGS_FOLDER}/globalpay_processing/'
                                       f'{SQL_FOLDER}/reopen_store.sql', deploy_env_name)
global_payment_merchant_schema = read_variable_or_file(f'globalpay_processing/{GLOBAL_PAYMENT_MERCHANT_SCHEMA}',
                                                       deploy_env_name)

DAG_DEFAULT_ARGS = {
    'owner': 'team-convergence-alerts',
    'capability': 'Loyalty',
    'severity': 'P2',
    'sub_capability': 'Rewards/Loyalty',
    'business_impact': (
        'This will impact the loading of LCL/SDM merchants. Campaign service merchant list will be out '
        'of sync and all new LCL/ SDM merchants will not be updated in campaign service.'
    ),
    'customer_impact': (
        'All transactions in new LCL/SDM stores will be considered out of network store transaction. '
        'Customer will have a lower points earn rate.'
    ),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 3,
    'retry_delay': timedelta(seconds=10)
}


def run_update_store_list_sql(table_name):
    if not check_bq_table_exists(table_name):
        logger.info("Creating GP Merchant table.")
        run_bq_query(create_table_query)

    logger.info("Inserting new stores to the GP Merchant table.")
    run_bq_query(new_store_query)

    logger.info("Updating deactivated stores in the GP Merchant table.")
    run_bq_query(deactivate_store_query)

    logger.info("Updating reopened stores in the GP Merchant table.")
    run_bq_query(reopen_store_query)

    logger.info("Finished processing the file.")


with DAG(dag_id=DAG_ID,
         default_args=DAG_DEFAULT_ARGS,
         schedule=None,
         description=(
             'DAG to load the Global Payment Merchant list from Cloud Storage to BigQuery and DP Kafka. Will be used '
             'by DP to awarded points to customers.'
         ),
         render_template_as_native_obj=True,
         start_date=datetime(2023, 1, 1, tzinfo=local_tz),
         max_active_runs=1,
         catchup=False,
         dagrun_timeout=timedelta(hours=24),
         is_paused_upon_creation=True
         ) as dag:
    if glp_pcl_lcl_merch_list_config[consts.READ_PAUSE_DEPLOY_CONFIG]:
        is_paused = read_pause_unpause_setting(DAG_ID, deploy_env_name)
        pause_unpause_dag(dag, is_paused)

    staging_project = gcp_config.get(consts.PROCESSING_ZONE_PROJECT_ID)
    staging_dataset = glp_pcl_lcl_merch_list_config.get(consts.STAGING_DATASET)
    staging_table = glp_pcl_lcl_merch_list_config.get(consts.STAGING_TABLE_ID)

    start_point = EmptyOperator(task_id=consts.START_TASK_ID)
    end_point = EmptyOperator(task_id=consts.END_TASK_ID)

    loading_task = GCSToBigQueryOperator(
        task_id=consts.LOADING_TASK,
        bucket="{{ dag_run.conf['bucket'] }}",
        source_objects="{{ dag_run.conf['name'] }}",
        destination_project_dataset_table=f'{staging_project}.{staging_dataset}.{staging_table}',
        schema_fields=global_payment_merchant_schema[consts.SCHEMA],
        write_disposition=consts.WRITE_TRUNCATE,
        field_delimiter=glp_pcl_lcl_merch_list_config.get(consts.FIELD_DELIMITER),
        time_partitioning={
            consts.TYPE: glp_pcl_lcl_merch_list_config.get(consts.TIME_PARTITIONING_TYPE),
            consts.FIELD: glp_pcl_lcl_merch_list_config.get(consts.TIME_PARTITIONING_FIELD)
        },
        cluster_fields=[glp_pcl_lcl_merch_list_config.get(consts.CLUSTER_FIELDS)],
        location=gcp_config.get(consts.BQ_QUERY_LOCATION)
    )

    update_store_list_task = PythonOperator(
        task_id=consts.UPDATING_STORE_LIST_TASK_ID,
        python_callable=run_update_store_list_sql,
        op_kwargs={
            consts.TABLE_NAME: glp_pcl_lcl_merch_list_config.get(consts.TABLE_ID)
        }
    )

    gcs_to_gcs_task = GCSToGCSOperator(
        task_id=consts.GCS_TO_GCS,
        gcp_conn_id=gcp_config.get(consts.LANDING_ZONE_CONNECTION_ID),
        source_bucket="{{ dag_run.conf['bucket'] }}",
        source_object="{{ dag_run.conf['name'] }}",
        destination_bucket=glp_pcl_lcl_merch_list_config.get(consts.STAGING_BUCKET),
        destination_object="{{ dag_run.conf['name']}}",
    )

    kafka_writer_task = TriggerDagRunOperator(
        task_id=consts.KAFKA_WRITER_TASK,
        trigger_dag_id=glp_pcl_lcl_merch_list_config.get(consts.KAFKA_TRIGGER_DAG_ID),
        logical_date=datetime.now(local_tz),
        conf={
            consts.BUCKET: glp_pcl_lcl_merch_list_config.get(consts.STAGING_BUCKET),
            consts.NAME: "{{ dag_run.conf['name'] }}",
            consts.FOLDER_NAME: "{{ dag_run.conf['folder_name'] }}",
            consts.FILE_NAME: "{{ dag_run.conf['file_name'] }}",
            consts.CLUSTER_NAME: glp_pcl_lcl_merch_list_config.get(consts.KAFKA_CLUSTER_NAME)
        },
        wait_for_completion=True,
        poke_interval=30
    )

    start_point >> loading_task >> update_store_list_task >> gcs_to_gcs_task >> kafka_writer_task >> end_point
