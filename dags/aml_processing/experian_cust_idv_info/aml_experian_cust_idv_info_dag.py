from datetime import datetime, timedelta
import copy
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from aml_processing import feed_commons as commons
from aml_processing.experian_cust_idv_info.aml_experian_cust_idv_info import ExperianCustIdvInfo

# SCHEMA PATH
AML_IDV_EXPERIAN_DATA_SCHEMA_PATH = f'{settings.DAGS_FOLDER}/aml_processing/experian_cust_idv_info/AML_IDV_EXPERIAN_DATA_SCHEMA.json'

tags = commons.TAGS
tags.append("experian")
# Modify the default_args dictionary
custom_default_args = copy.deepcopy(commons.DAG_DEFAULT_ARGS)
custom_default_args['business_impact'] = 'Customer identity information in Verafin will not be up-to-date if this process fails'
docs = 'This DAG loads Experian IDV Data to curated CUST_IDV_INFO'
tags.append(custom_default_args['severity'])

with DAG(
        dag_id='aml_experian_to_cust_idv_info_dag',
        default_args=custom_default_args,
        description='DAG to load Experian IDV Data to curated CUST_IDV_INFO',
        dagrun_timeout=timedelta(hours=23, minutes=30),
        catchup=False,
        start_date=datetime(2023, 1, 1, tzinfo=commons.TORONTO_TZ),
        tags=tags,
        doc_md=docs,
        schedule=None,
        max_active_runs=1,
        is_paused_upon_creation=True
) as dag:

    experian_cust_idv_info = ExperianCustIdvInfo()
    pipeline_start = EmptyOperator(task_id='pipeline_start')

    create_exp_base_table_task = PythonOperator(
        task_id='task_create_exp_base_table',
        python_callable=experian_cust_idv_info.task_create_exp_base_table,
        op_args=['aml_processing/experian_cust_idv_info/aml_exp_base_creation_query.sql']
    )

    create_exp_acc_cust_table_task = PythonOperator(
        task_id='task_create_exp_acc_cust_table',
        python_callable=experian_cust_idv_info.task_create_exp_acc_cust_table,
        op_args=['aml_processing/experian_cust_idv_info/aml_exp_acc_cust_query.sql']
    )

    create_aml_idv_experian_data_task = PythonOperator(
        task_id='task_create_aml_idv_experian_data',
        python_callable=experian_cust_idv_info.task_create_aml_idv_experian_data,
        op_args=[AML_IDV_EXPERIAN_DATA_SCHEMA_PATH]
    )

    insert_into_aml_idv_experian_data_task = PythonOperator(
        task_id='task_insert_into_aml_idv_experian_data',
        python_callable=experian_cust_idv_info.task_insert_into_aml_idv_experian_data,
        op_args=['aml_processing/experian_cust_idv_info/aml_insert_into_idv_experian_data_query.sql']
    )

    delete_existing_experian_data_from_cust_idv_info_task = PythonOperator(
        task_id='task_delete_existing_experian_data_from_cust_idv_info',
        python_callable=experian_cust_idv_info.task_delete_existing_experian_data_from_cust_idv_info,
        op_args=['aml_processing/experian_cust_idv_info/aml_delete_existing_experian_data_from_cust_idv_info_query.sql']
    )

    insert_into_cust_idv_info_task = PythonOperator(
        task_id='task_insert_into_cust_idv_info',
        python_callable=experian_cust_idv_info.task_insert_into_cust_idv_info,
        op_args=['aml_processing/experian_cust_idv_info/aml_exp_insert_into_cust_idv_info_query.sql']
    )

    pipeline_complete = EmptyOperator(
        task_id='pipeline_complete', trigger_rule='all_success')

    pipeline_start >> \
        create_exp_base_table_task >> \
        create_exp_acc_cust_table_task >> \
        create_aml_idv_experian_data_task >> \
        insert_into_aml_idv_experian_data_task >> \
        delete_existing_experian_data_from_cust_idv_info_task >> \
        insert_into_cust_idv_info_task >> \
        pipeline_complete
