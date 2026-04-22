import logging
from datetime import datetime, date, timedelta
from dataclasses import dataclass
from typing import List
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from google.cloud import bigquery
from util.miscutils import read_variable_or_file
from util.deploy_utils import read_pause_unpause_setting
from aml_processing import feed_commons as commons
from aml_processing.transaction.bq_util import run_bq_select, run_bq_dml_with_log
from aml_processing.transaction.agg_domain_at31 import sql_account_info, sql_primary_account_info, \
    sql_stg_merchant_info, \
    sql_process_agg_at31, sql_create_agg_domain_at31_table, sql_create_control_table, AT31_TRANSACTION_TABLE, \
    BQTABLE_TRANSACTION_AGG_CONTROL, AGG_BQTABLE_AT31, STG_BQTABLE_CARD_OWNER, STG_BQTABLE_PRIMARY_ACCOUNT_INFO, \
    STG_BQTABLE_MERCHANT_INFO, STG_REF_RAIL_NO_DP_REF_NO, STG_REF_RAIL_WITH_DP_REF_NO, sql_delete_agg_at31, \
    sql_stg_ref_rail_type_no_dp_ref_no, sql_stg_ref_rail_type_with_dp_ref_no
from aml_processing.transaction.agg_control import log_task_start, log_task_complete, log_job_start, log_job_end
from aml_processing.aml_utils import AMLUtils


@dataclass
class FeedLoadMeta:
    file_create_dt: date
    rec_load_timestamp: datetime


# Constants
DEPLOY_ENV = read_variable_or_file('gcp_config')['deployment_environment_name']
DAG_NAME = "aml-transaction-agg-domain-at31"
START_DATE = datetime(2022, 1, 1, tzinfo=commons.TORONTO_TZ)  # days_ago(1).replace(tzinfo=LOCAL_TZ)

IS_PAUSED = read_pause_unpause_setting(
    DAG_NAME, DEPLOY_ENV)  # Read the settings from yaml


def get_feed_load_metas(dag_run_id: str) -> List[FeedLoadMeta]:
    sql = f"""
          SELECT C.FILE_CREATE_DT,C.REC_LOAD_TIMESTAMP
          FROM {BQTABLE_TRANSACTION_AGG_CONTROL} C
          WHERE C.RUN_ID = @dag_run_id
          AND C.LATEST_VERSION = 'Y'
          ORDER BY C.FILE_CREATE_DT
        """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("dag_run_id", "STRING", dag_run_id)
        ]
    )
    feed_load_meta_list: List[FeedLoadMeta] = []
    result = run_bq_select(sql, job_config=job_config)
    for row in result:
        feed_load_meta_list.append(FeedLoadMeta(row['FILE_CREATE_DT'], row['REC_LOAD_TIMESTAMP']))
    return feed_load_meta_list


def task_stg_account_info():
    sql = sql_account_info()
    run_bq_dml_with_log(f"Creating staging table:{STG_BQTABLE_CARD_OWNER}",
                        f"Completed staging table:{STG_BQTABLE_CARD_OWNER}", sql)


def task_stg_merchant_info():
    sql = sql_stg_merchant_info()
    run_bq_dml_with_log(f"Creating staging table:{STG_BQTABLE_MERCHANT_INFO}",
                        f"Completed staging table:{STG_BQTABLE_MERCHANT_INFO}", sql)


def task_stg_primary_account_info():
    sql = sql_primary_account_info()
    run_bq_dml_with_log(f"Creating staging table:{STG_BQTABLE_PRIMARY_ACCOUNT_INFO}",
                        f"Completed staging table:{STG_BQTABLE_PRIMARY_ACCOUNT_INFO}", sql)


def task_stg_ref_rail_type_no_dp_ref_no():
    sql = sql_stg_ref_rail_type_no_dp_ref_no()
    run_bq_dml_with_log(f"Creating staging table:{STG_REF_RAIL_NO_DP_REF_NO}",
                        f"Completed staging table:{STG_REF_RAIL_NO_DP_REF_NO}", sql)


def task_stg_ref_rail_type_with_dp_ref_no():
    sql = sql_stg_ref_rail_type_with_dp_ref_no()
    run_bq_dml_with_log(f"Creating staging table:{STG_REF_RAIL_WITH_DP_REF_NO}",
                        f"Completed staging table:{STG_REF_RAIL_WITH_DP_REF_NO}", sql)


def task_create_agg_domain_at31_table():
    sql = sql_create_agg_domain_at31_table()
    run_bq_dml_with_log(f"Creating aml agg domain at31 table:{AGG_BQTABLE_AT31}",
                        f"Completed aml agg domain at31 table:{AGG_BQTABLE_AT31}", sql)


def task_create_control_table():
    sql = sql_create_control_table()
    run_bq_dml_with_log(f"Creating aml agg control table:{BQTABLE_TRANSACTION_AGG_CONTROL}",
                        f"Completed aml agg control table:{BQTABLE_TRANSACTION_AGG_CONTROL}", sql)


def task_load_to_agg_at31(**context):
    #
    #
    dag_run_id = context['run_id']
    feed_load_meta_list: List[FeedLoadMeta] = get_feed_load_metas(dag_run_id)

    if len(feed_load_meta_list) == 0:
        logging.info("There is no data to load from at31 landing")
        return

    sql = sql_process_agg_at31()
    delete_sql = sql_delete_agg_at31()

    batch_size = 30
    feed_load_meta_list_chunks = [feed_load_meta_list[x:x + batch_size]
                                  for x in range(0, len(feed_load_meta_list), batch_size)]

    for chunk in feed_load_meta_list_chunks:
        file_create_dt_list = [x.file_create_dt for x in chunk]
        file_create_dt_str_list = [x.strftime("%Y-%m-%d") for x in file_create_dt_list]

        logging.info(
            f"loading landing records for file_create_dt list:{file_create_dt_str_list}")

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("dag_run_id", "STRING", dag_run_id),
                bigquery.ArrayQueryParameter("file_create_dt_list", "DATE", file_create_dt_list),
            ]
        )
        log_task_start(dag_run_id, file_create_dt_list)

        run_bq_dml_with_log(f"Deleting aggregated at31 for file_create_dt list:{file_create_dt_str_list}",
                            f"Completed deleting aggregate at31 for file_create_dt list:{file_create_dt_str_list}",
                            delete_sql, job_config)
        run_bq_dml_with_log(f"Loading to aggregated AT31,file_create_dt list:{file_create_dt_str_list}",
                            f"Completed loading to aggregated AT31,file_create_dt_str:{file_create_dt_str_list}",
                            sql, job_config)
        log_task_complete(dag_run_id, file_create_dt_list)


def task_log_job_start(**context):
    dag_run_id = context['run_id']
    start_date_txt = context['params']['start_date']
    end_date_txt = context['params']['end_date']
    start_date = AMLUtils.parse_date(start_date_txt)
    end_date = AMLUtils.parse_date(end_date_txt)

    if start_date is None:
        start_date = end_date
    if end_date is None:
        end_date = start_date

    if start_date is not None and end_date is not None and end_date < start_date:
        raise AirflowFailException(f"{end_date_txt} is earlier than {start_date_txt}")

    log_job_start(dag_run_id, start_date, end_date)


def task_log_job_complete(**context):
    dag_run_id = context['run_id']
    log_job_end(dag_run_id)


tags = commons.TAGS
tags.append("AT31")
docs = f"""This DAG loads at31 transactions from landing {AT31_TRANSACTION_TABLE} to curate zone: {AGG_BQTABLE_AT31}
            parameters:
               start_date: the date range start date to filter the records against field 'file_create_dt' with format
                           'yyyy-MM-dd', default to None
               end_date: the date range end date to filter the records against field 'file_create_dt' with format
               'yyyy-MM-dd', default to None
            By default, these two parameters are set to None, DAG loads all those records that have not been processed
            automatically.
            If these the date range is set, DAG will load only records that fall into the period.
            For the same run, if there is multiple version data for the same file_create_dt, only the latest version
            will be loaded.
        """

# Modify the default_args dictionary
custom_default_args = commons.DAG_DEFAULT_ARGS.copy()
custom_default_args['business_impact'] = 'Posted transactions needed for AML analysis will be missing in Verafin if this process fails'
custom_default_args['severity'] = 'P2'
tags.append(custom_default_args['severity'])

with DAG(
        DAG_NAME,
        params={
            "start_date": None,
            "end_date": None
        },
        default_args=custom_default_args,
        description="DAG to load to AML Transaction AT31 Agg Domain",
        schedule='0 6 * * 2-6',
        dagrun_timeout=timedelta(hours=23, minutes=30),
        catchup=False,
        start_date=START_DATE,
        tags=tags,
        doc_md=docs,
        max_active_runs=1,
        is_paused_upon_creation=True
) as dag:
    pipeline_start = EmptyOperator(task_id="pipeline_start")

    create_control_table_task = PythonOperator(
        task_id="create_control_table_task",
        python_callable=task_create_control_table,
        op_args=[],
        trigger_rule="all_success"
    )

    log_job_start_task = PythonOperator(
        task_id="log_job_start_task",
        python_callable=task_log_job_start,
        op_args=[],
        trigger_rule="all_success"
    )

    create_staging_table_account_info_task = PythonOperator(
        task_id="create_staging_table_account_info_task",
        python_callable=task_stg_account_info,
        op_args=[],
        trigger_rule="all_success"
    )

    create_staging_table_primary_account_info_task = PythonOperator(
        task_id="create_staging_table_primary_account_info_task",
        python_callable=task_stg_primary_account_info,
        op_args=[],
        trigger_rule="all_success"
    )

    create_staging_merchant_info_task = PythonOperator(
        task_id="create_staging_merchant_info_task",
        python_callable=task_stg_merchant_info,
        op_args=[],
        trigger_rule="all_success"
    )

    create_staging_ref_rail_with_ref_dp_no_task = PythonOperator(
        task_id="create_staging_ref_rail_with_ref_dp_no_task",
        python_callable=task_stg_ref_rail_type_with_dp_ref_no,
        op_args=[],
        trigger_rule="all_success"
    )

    create_staging_ref_rail_no_ref_dp_no_task = PythonOperator(
        task_id="create_staging_ref_rail_no_ref_dp_no_task",
        python_callable=task_stg_ref_rail_type_no_dp_ref_no,
        op_args=[],
        trigger_rule="all_success"
    )

    create_agg_domain_at31_table_task = PythonOperator(
        task_id="create_agg_domain_at31_table_task",
        python_callable=task_create_agg_domain_at31_table,
        op_args=[],
        trigger_rule="all_success"
    )

    load_to_agg_at31_task = PythonOperator(
        task_id="load_to_agg_at31_task",
        python_callable=task_load_to_agg_at31,
        op_args=[],
        execution_timeout=timedelta(hours=23),
        trigger_rule="all_success"
    )

    log_job_complete_task = PythonOperator(
        task_id="log_job_complete_task",
        python_callable=task_log_job_complete,
        op_args=[],
        trigger_rule="all_success"
    )

    pipeline_complete = EmptyOperator(task_id="pipeline_complete", trigger_rule="all_success")

    pipeline_start >> create_control_table_task >> \
        log_job_start_task >> \
        create_staging_table_account_info_task >> \
        create_staging_table_primary_account_info_task >> \
        create_staging_merchant_info_task >> \
        create_staging_ref_rail_with_ref_dp_no_task >> \
        create_staging_ref_rail_no_ref_dp_no_task >> \
        create_agg_domain_at31_table_task >> \
        load_to_agg_at31_task >> \
        log_job_complete_task >> pipeline_complete
