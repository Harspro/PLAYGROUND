import pendulum
import logging
import copy
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from airflow import settings
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from aml_processing import feed_commons as commons
from util.bq_utils import run_bq_query
from util.miscutils import (
    read_variable_or_file,
    read_env_filepattern,
    read_yamlfile_env_suffix
)
import util.constants as consts
from aml_processing.transaction.eve.transaction_eve_feed import sql_insert_into_eve_table, \
    sql_create_eve_transaction_feed_table, build_control_record_saving_job, \
    check_control_table_updates, insert_into_curated_transaction_feed_historical_records_table
from aml_processing.transaction.transaction_feed_extractor_base import TBL_CURATED_TRANSACTION_FEED_HISTORICAL_SCHEMA

gcp_config = read_variable_or_file(consts.GCP_CONFIG)
deploy_env = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
deploy_env_suffix = gcp_config['deploy_env_storage_suffix']
config_dir = f'{settings.DAGS_FOLDER}/aml_processing/transaction/eve/config'
job_config = read_yamlfile_env_suffix(fname=f'{config_dir}/eve_transaction_config.yaml',
                                      deploy_env=deploy_env,
                                      env_suffix=deploy_env_suffix)
OUTBOUND_FILE_EXTENSION = read_env_filepattern(job_config['filename_extension'], deploy_env)
TORONTO_TZ = pendulum.timezone('America/Toronto')
CURRENT_DATETIME = datetime.now(tz=TORONTO_TZ)
JOB_DATE = f"{CURRENT_DATETIME.strftime('%Y%m%d')}"
JOB_DATE_TIME = f"{CURRENT_DATETIME.strftime('%Y%m%d%H%M%S')}"
FILENAME_FORMAT_WITH_DATE = f"{job_config['filename_prefix']}_{JOB_DATE}" \
                            f".{OUTBOUND_FILE_EXTENSION}"
FILENAME_FORMAT_WITH_DATETIME = f"{job_config['filename_prefix']}_{JOB_DATE_TIME}" \
                                f".{OUTBOUND_FILE_EXTENSION}"
OUTBOUND_FILENAME = f"{job_config['outbound_folder']}/{FILENAME_FORMAT_WITH_DATETIME}"


logger = logging.getLogger(__name__)
DAG_NAME = job_config['dag_name']
custom_default_args = copy.deepcopy(commons.DAG_DEFAULT_ARGS)
custom_default_args['business_impact'] = 'Posted transactions needed for AML analysis will be missing in Verafin if this process fails'
START_DATE = datetime(2022, 1, 1, tzinfo=commons.TORONTO_TZ)
tags = commons.TAGS.copy()
tags.append(custom_default_args['severity'])

docs = """
    This is part of verafin transaction feeds. Eve handles the SAVINGS account transactions.
"""


def task_load_into_eve_feed(job_config, **context):
    ti = context['task_instance']
    start_time = ti.xcom_pull(task_ids='check_control_table_updates_task', key='start_time')
    end_time = ti.xcom_pull(task_ids='check_control_table_updates_task', key='end_time')

    logger.info(f'The run intervals are {start_time} and {end_time} ')
    sql = sql_insert_into_eve_table(job_config, start_time, end_time)
    logger.info(f"Running Insert into Eve table : {job_config['bqtable_eve_transaction_feed']} using query : {sql}")
    run_bq_query(sql)


def task_create_table_transaction_eve_feed(job_config, **context):
    sql = sql_create_eve_transaction_feed_table(job_config, TBL_CURATED_TRANSACTION_FEED_HISTORICAL_SCHEMA)
    logger.info(f"Running Create Eve table : {job_config['bqtable_eve_transaction_feed']} using query : {sql}")
    run_bq_query(sql)


with DAG(
        DAG_NAME,
        default_args=custom_default_args,
        description="DAG to load EVE transactions for AML",
        schedule='05 11 * * 2-6',
        catchup=False,
        params={
            "start_time": "",
            "end_time": ""
        },
        start_date=START_DATE,
        tags=tags,
        doc_md=docs,
        max_active_runs=1,
        is_paused_upon_creation=True
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    check_control_table_updates_task = PythonOperator(
        task_id="check_control_table_updates_task",
        python_callable=check_control_table_updates,
        op_args=[job_config],
        dag=dag
    )

    create_table_transaction_eve_feed_task = PythonOperator(
        task_id="create_table_eve_transaction_feed_task",
        python_callable=task_create_table_transaction_eve_feed,
        op_args=[job_config],
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    load_into_eve_feed_task = PythonOperator(
        task_id="load_into_eve_feed_task",
        python_callable=task_load_into_eve_feed,
        op_args=[job_config],
        execution_timeout=timedelta(hours=23),
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    export_table_to_gcs = PythonOperator(
        task_id="export_table_eve_transaction_feed_to_gcs_task",
        python_callable=commons.export_bq_table,
        op_args=[job_config['staging_bucket'], job_config['bq_to_gcs_folder'],
                 job_config['destination_bq_dataset'], job_config['bq_table_name'], False, True],
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    compose_feed_file = PythonOperator(
        task_id="compose_feed_file",
        python_callable=commons.compose_infinite_files_into_one,
        op_args=[job_config['staging_bucket'], job_config['staging_folder'],
                 job_config['source_headers_file_path'], job_config['destination_headers_file_path'],
                 job_config['bq_to_gcs_folder'], job_config['bq_table_name']],
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    copy_object_to_outbound_bucket_task = PythonOperator(
        task_id="copy_object_to_outbound_bucket_task",
        python_callable=commons.copy_object,
        op_args=[job_config['staging_bucket'], job_config['staging_folder'],
                 job_config['outbound_bucket'], OUTBOUND_FILENAME],
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    insert_into_curated_transaction_feed_historical_records_table_task = PythonOperator(
        task_id="insert_into_curated_transaction_feed_historical_records_table_task",
        python_callable=insert_into_curated_transaction_feed_historical_records_table,
        op_args=[job_config['feed_name'], job_config],
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    control_record_saving_task = PythonOperator(
        task_id="save_job_to_control_table",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        python_callable=build_control_record_saving_job,
        op_args=[job_config['dag_name']],
        dag=dag
    )

    start >> check_control_table_updates_task >> \
        create_table_transaction_eve_feed_task >> \
        load_into_eve_feed_task >> \
        export_table_to_gcs >> \
        compose_feed_file >> \
        copy_object_to_outbound_bucket_task >> \
        insert_into_curated_transaction_feed_historical_records_table_task >> \
        control_record_saving_task >> \
        end
