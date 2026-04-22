from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from aml_processing import feed_commons as commons
from aml_processing.abb_cust_idv_info.aml_abb_cust_info import ABBIdvInfo

tags = commons.TAGS
tags.append("abb")
# Modify the default_args dictionary
custom_default_args = commons.DAG_DEFAULT_ARGS.copy()
custom_default_args['business_impact'] = 'Customer identity information in Verafin will not be up-to-date if this process fails'
docs = "This DAG loads ABB flag to curate zone's CUST_IDV_INFO"
tags.append(custom_default_args['severity'])

with DAG(
        dag_id='aml_abb_dag',
        params={
            "start_date": "2023-01-01"
        },
        default_args=custom_default_args,
        description="DAG to load to AML ABB flag to curated idv_info(daily)",
        schedule='0 6 * * 1-5',
        dagrun_timeout=timedelta(hours=23, minutes=30),
        catchup=False,
        start_date=datetime(2023, 1, 1, tzinfo=commons.TORONTO_TZ),
        tags=tags,
        doc_md=docs,
        max_active_runs=1,
        is_paused_upon_creation=True
) as dag:

    abb_idv_aml = ABBIdvInfo()
    pipeline_start = EmptyOperator(task_id="pipeline_start")

    create_app_abb_table_task = PythonOperator(
        task_id="task_create_app_abb_table",
        python_callable=abb_idv_aml.task_create_app_abb_table,
        op_args=["aml_processing/abb_cust_idv_info/app_abb_creation_query.sql"]
    )

    create_app_abb_acc_table_task = PythonOperator(
        task_id="task_create_app_abb_acc_table",
        python_callable=abb_idv_aml.task_create_app_abb_acc_table,
        op_args=["aml_processing/abb_cust_idv_info/app_abb_acc_table_query.sql"]
    )

    create_stg_abb_cust_task = PythonOperator(
        task_id="task_create_stg_abb_cust_table",
        python_callable=abb_idv_aml.task_create_stg_abb_cust_table,
        op_args=["aml_processing/abb_cust_idv_info/stg_abb_cust_query.sql"]
    )

    create_aml_abb_cust_task = PythonOperator(
        task_id="task_create_aml_abb_cust",
        python_callable=abb_idv_aml.task_create_aml_abb_cust,
        op_args=["aml_processing/abb_cust_idv_info/aml_abb_cust_creation_query.sql",
                 "aml_processing/abb_cust_idv_info/aml_abb_cust_insert_query.sql"]
    )

    insert_abb_cust_idv_info = PythonOperator(
        task_id="task_insert_abb_cust_idv_info",
        python_callable=abb_idv_aml.task_insert_abb_cust_idv_info,
        op_args=["aml_processing/abb_cust_idv_info/cust_idv_info_abb_insert_query.sql"]
    )

    pipeline_complete = EmptyOperator(
        task_id="pipeline_complete", trigger_rule="all_success")

    pipeline_start >> \
        create_app_abb_table_task >> \
        create_app_abb_acc_table_task >> \
        create_stg_abb_cust_task >> \
        create_aml_abb_cust_task >> \
        insert_abb_cust_idv_info >> \
        pipeline_complete
