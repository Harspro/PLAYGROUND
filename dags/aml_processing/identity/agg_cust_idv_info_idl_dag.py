from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from google.cloud import bigquery

from util.miscutils import read_variable_or_file

from aml_processing import feed_commons as commons
from aml_processing.transaction.bq_util import run_bq_dml_with_log
from aml_processing.identity.agg_cust_idv_info import sql_insert_from_fsc_agg_cust_idv_info, sql_insert_from_nftf_agg_cust_idv_info, sql_create_agg_cust_idv_info_table, sql_check_for_duplicates_in_source_tables, FSC_PARTY_IDV_INFO_TABLE, FSC_PARTY_IDV_NFTF_INFO_TABLE, AGG_CUST_IDV_INFO

from util.bq_utils import run_bq_query

logger = logging.getLogger(__name__)

# CONSTANTS
DEPLOY_ENV = read_variable_or_file('gcp_config')['deployment_environment_name']
DAG_NAME = "aml_agg_cust_idv_info_idl_dag"
START_DATE = datetime(2023, 1, 1, tzinfo=commons.TORONTO_TZ)


def task_create_agg_cust_idv_info_table():
    sql = sql_create_agg_cust_idv_info_table()
    run_bq_dml_with_log(f"Creating aml agg domain cust_idv_info table:{AGG_CUST_IDV_INFO}",
                        f"Completed aml agg domain cust_idv_info table:{AGG_CUST_IDV_INFO}", sql)


def task_check_for_duplicates_in_source_tables():
    sql = sql_check_for_duplicates_in_source_tables()
    logger.info(f"sql statement: {sql}")
    query_result = run_bq_query(sql)
    row_count = query_result.result().total_rows
    logger.info(f"Duplicate check count: {row_count}")
    if (row_count != 0):
        logger.error("DAG failed due to duplicate customer numbers in the source tables.")
        raise AirflowFailException("Duplicate Customer Numbers found in the source tables.")
    else:
        pass


def task_insert_from_ftf_to_agg_cust_idv_info(**context):
    #
    #
    sql = sql_insert_from_fsc_agg_cust_idv_info()
    job_config = None
    run_bq_dml_with_log(f"Loading from ftf to aggregated {AGG_CUST_IDV_INFO}",
                        f"Completed loading to aggregated {AGG_CUST_IDV_INFO}",
                        sql, job_config)


def task_insert_from_nftf_to_agg_cust_idv_info(**context):
    #
    #
    sql = sql_insert_from_nftf_agg_cust_idv_info()
    job_config = None
    run_bq_dml_with_log(f"Loading from nftf to aggregated {AGG_CUST_IDV_INFO}",
                        f"Completed loading to aggregated {AGG_CUST_IDV_INFO}",
                        sql, job_config)


tags = commons.TAGS
tags.append("identity")
docs = f"""This DAG loads identity records from landing {FSC_PARTY_IDV_INFO_TABLE} & {FSC_PARTY_IDV_NFTF_INFO_TABLE} to curated zone:       {AGG_CUST_IDV_INFO}"""

# Modify the default_args dictionary
custom_default_args = commons.DAG_DEFAULT_ARGS.copy()
custom_default_args['business_impact'] = 'Customer identity information in Verafin will not be up-to-date if this process fails'
tags.append(custom_default_args['severity'])

with DAG(
        DAG_NAME,
        default_args=custom_default_args,
        description="DAG to load to AML Identity aggregate CUST_IDV_INFO",
        dagrun_timeout=timedelta(hours=23, minutes=30),
        catchup=False,
        start_date=START_DATE,
        tags=tags,
        doc_md=docs,
        schedule=None,
        max_active_runs=1,
        is_paused_upon_creation=True
) as dag:
    pipeline_start = EmptyOperator(task_id="pipeline_start")

    check_for_duplicates_in_source_tables_task = PythonOperator(
        task_id="check_for_duplicates_in_source_tables_task",
        python_callable=task_check_for_duplicates_in_source_tables,
        op_args=[],
        trigger_rule="all_success"
    )

    create_agg_cust_idv_info_table_task = PythonOperator(
        task_id="create_agg_cust_idv_info_table_task",
        python_callable=task_create_agg_cust_idv_info_table,
        op_args=[],
        trigger_rule="all_success"
    )

    insert_from_ftf_to_agg_cust_idv_info_task = PythonOperator(
        task_id="insert_from_ftf_to_agg_cust_idv_info_task",
        python_callable=task_insert_from_ftf_to_agg_cust_idv_info,
        op_args=[],
        execution_timeout=timedelta(hours=23),
        trigger_rule="all_success"
    )

    insert_from_nftf_to_agg_cust_idv_info_task = PythonOperator(
        task_id="insert_from_nftf_to_agg_cust_idv_info_task",
        python_callable=task_insert_from_nftf_to_agg_cust_idv_info,
        op_args=[],
        execution_timeout=timedelta(hours=23),
        trigger_rule="all_success"
    )

    pipeline_complete = EmptyOperator(
        task_id="pipeline_complete", trigger_rule="all_success")

    pipeline_start >> check_for_duplicates_in_source_tables_task >> create_agg_cust_idv_info_table_task >> insert_from_ftf_to_agg_cust_idv_info_task >> insert_from_nftf_to_agg_cust_idv_info_task >> pipeline_complete
