from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from aml_processing import feed_commons as commons
from aml_processing.aml_to_cust_idv_info.aml_to_cust_idv_common import AmlCustIdvConst
from aml_processing.aml_to_cust_idv_info.aml_to_cust_idv_info import AmlToCustIdvInfo


tags = commons.TAGS
tags.append("identity")
docs = f"""This DAG loads identity records from `{AmlCustIdvConst.AML_IDV_INFO_TABLE}` table to {AmlCustIdvConst.AGG_CUST_IDV_INFO_TABLE} table"""

# Modify the default_args dictionary
custom_default_args = commons.DAG_DEFAULT_ARGS.copy()
custom_default_args['business_impact'] = 'Customer identity information in Verafin will not be up-to-date if this process fails'

with DAG(
        'aml_to_cust_idv_info',
        default_args=custom_default_args,
        description=f"DAG to load to AML Customer Identity Table {AmlCustIdvConst.AGG_CUST_IDV_INFO_TABLE}",
        dagrun_timeout=timedelta(hours=23, minutes=30),
        catchup=False,
        start_date=AmlCustIdvConst.DAG_START_DATE,
        tags=tags,
        doc_md=docs,
        schedule=None,
        max_active_runs=1,
        is_paused_upon_creation=True
) as dag:
    aml_cust_idv_info = AmlToCustIdvInfo()
    pipeline_start = EmptyOperator(task_id="pipeline_start")

    check_for_duplicates_in_source_tables_task = PythonOperator(
        task_id="check_for_duplicates_in_source_tables_task",
        python_callable=aml_cust_idv_info.task_check_for_duplicates_in_source_tables,
        op_args=[],
        trigger_rule="all_success"
    )

    create_control_table_task = PythonOperator(
        task_id="create_control_table_task",
        python_callable=aml_cust_idv_info.task_create_ctl_table,
        op_args=[],
        trigger_rule="all_success"
    )

    insert_into_cust_idv_info_table_task = PythonOperator(
        task_id="insert_into_cust_idv_info_table_task",
        python_callable=aml_cust_idv_info.task_insert_into_cust_idv_info_table,
        op_args=[],
        trigger_rule="all_success"
    )

    update_control_table_task = PythonOperator(
        task_id="update_control_table_task",
        python_callable=aml_cust_idv_info.task_update_ctl_table,
        op_args=[],
        trigger_rule="all_success"
    )

    pipeline_complete = EmptyOperator(
        task_id="pipeline_complete", trigger_rule="all_success")

    pipeline_start >> \
        check_for_duplicates_in_source_tables_task >> \
        create_control_table_task >> \
        insert_into_cust_idv_info_table_task >> \
        update_control_table_task >> \
        pipeline_complete
