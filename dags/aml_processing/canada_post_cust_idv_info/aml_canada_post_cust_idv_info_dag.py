from datetime import datetime, timedelta
import copy
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from aml_processing import feed_commons as commons
from aml_processing.canada_post_cust_idv_info.aml_canada_post_cust_idv_info import CanadaPostCustIdvInfo

# SCHEMA PATH
AML_IDV_CANADA_POST_DATA_SCHEMA_PATH = f'{settings.DAGS_FOLDER}/aml_processing/canada_post_cust_idv_info/AML_IDV_CANADA_POST_DATA_SCHEMA.json'

tags = commons.TAGS
tags.append("canada_post")
# Modify the default_args dictionary
custom_default_args = copy.deepcopy(commons.DAG_DEFAULT_ARGS)
custom_default_args['business_impact'] = 'Customer identity information in Verafin will not be up-to-date if this process fails'
docs = 'This DAG loads Canada Post IDV Data to curated CUST_IDV_INFO'
tags.append(custom_default_args['severity'])

with DAG(
        dag_id='aml_canada_post_to_cust_idv_info_dag',
        default_args=custom_default_args,
        description='DAG to load Canada Post IDV Data to curated CUST_IDV_INFO',
        dagrun_timeout=timedelta(hours=23, minutes=30),
        catchup=False,
        start_date=datetime(2023, 1, 1, tzinfo=commons.TORONTO_TZ),
        tags=tags,
        doc_md=docs,
        schedule=None,
        max_active_runs=1,
        is_paused_upon_creation=True
) as dag:

    canada_post_cust_idv_info = CanadaPostCustIdvInfo()
    pipeline_start = EmptyOperator(task_id='pipeline_start')

    create_cp_base_table_task = PythonOperator(
        task_id='task_create_cp_base_table',
        python_callable=canada_post_cust_idv_info.task_create_cp_base_table,
        op_args=['aml_processing/canada_post_cust_idv_info/aml_cp_base_creation_query.sql']
    )

    create_cp_acc_cust_table_task = PythonOperator(
        task_id='task_create_cp_acc_cust_table',
        python_callable=canada_post_cust_idv_info.task_create_cp_acc_cust_table,
        op_args=['aml_processing/canada_post_cust_idv_info/aml_cp_acc_cust_query.sql']
    )

    create_aml_idv_canada_post_data_task = PythonOperator(
        task_id='task_create_aml_idv_canada_post_data',
        python_callable=canada_post_cust_idv_info.task_create_aml_idv_canada_post_data,
        op_args=[AML_IDV_CANADA_POST_DATA_SCHEMA_PATH]
    )

    insert_into_aml_idv_canada_post_data_task = PythonOperator(
        task_id='task_insert_into_aml_idv_canada_post_data',
        python_callable=canada_post_cust_idv_info.task_insert_into_aml_idv_canada_post_data,
        op_args=['aml_processing/canada_post_cust_idv_info/aml_insert_into_idv_canada_post_data_query.sql']
    )

    delete_existing_canada_post_data_from_cust_idv_info_task = PythonOperator(
        task_id='task_delete_existing_canada_post_data_from_cust_idv_info',
        python_callable=canada_post_cust_idv_info.task_delete_existing_canada_post_data_from_cust_idv_info,
        op_args=['aml_processing/canada_post_cust_idv_info/aml_delete_existing_canada_post_data_from_cust_idv_info_query.sql']
    )

    insert_into_cust_idv_info_task = PythonOperator(
        task_id='task_insert_into_cust_idv_info',
        python_callable=canada_post_cust_idv_info.task_insert_into_cust_idv_info,
        op_args=['aml_processing/canada_post_cust_idv_info/aml_cp_insert_into_cust_idv_info_query.sql']
    )

    pipeline_complete = EmptyOperator(
        task_id='pipeline_complete', trigger_rule='all_success')

    pipeline_start >> \
        create_cp_base_table_task >> \
        create_cp_acc_cust_table_task >> \
        create_aml_idv_canada_post_data_task >> \
        insert_into_aml_idv_canada_post_data_task >> \
        delete_existing_canada_post_data_from_cust_idv_info_task >> \
        insert_into_cust_idv_info_task >> \
        pipeline_complete
