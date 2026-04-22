from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from aml_processing import feed_commons as commons
from aml_processing.adm_idv.adm_idv_common import ADMIdvConst
from aml_processing.adm_idv.adm_to_aml_idv_info import ADMIdvToAml

tags = commons.TAGS
tags.append("identity")
# Modify the default_args dictionary
custom_default_args = commons.DAG_DEFAULT_ARGS.copy()
custom_default_args['business_impact'] = 'Customer identity information in Verafin will not be up-to-date if this process fails'
docs = f"""This DAG loads IDV from landing {ADMIdvConst.TBL_LANDING_ADM_STD_DISP_APP_DATA} to curate zone: {ADMIdvConst.TBL_AGG_AML_IDV_INFO}
        """
tags.append(custom_default_args['severity'])

with DAG(
        'adm_idv_to_aml',
        params={
            "idl_cutoff_date": "2023-07-01"
        },
        default_args=custom_default_args,
        description="DAG to load to ADM IDV to curated AML(daily)",
        schedule='0 6 * * 1-5',
        dagrun_timeout=timedelta(hours=23, minutes=30),
        catchup=False,
        start_date=ADMIdvConst.DAG_START_DATE,
        tags=tags,
        doc_md=docs,
        max_active_runs=1,
        is_paused_upon_creation=True
) as dag:

    adm_idv_aml = ADMIdvToAml()
    pipeline_start = EmptyOperator(task_id="pipeline_start")

    create_control_table_task = PythonOperator(
        task_id="create_control_table_task",
        python_callable=adm_idv_aml.task_create_ctl_table,
        op_args=[],
        trigger_rule="all_success"
    )

    create_init_stg_ftf_nftf_task = PythonOperator(
        task_id="create_init_stg_ftf_nftf_task",
        python_callable=adm_idv_aml.task_create_init_stg_tables,
        op_args=[],
        trigger_rule="all_success"
    )

    idv_ftf_task = PythonOperator(
        task_id="idv_ftf_task",
        python_callable=adm_idv_aml.task_idv_ftf,
        op_args=[],
        trigger_rule="all_success"
    )

    idv_nftf_pav_task = PythonOperator(
        task_id="idv_nftf_pav_task",
        python_callable=adm_idv_aml.task_idv_nftf_pav,
        op_args=[],
        trigger_rule="all_success"
    )
    idv_nftf_single_source_task = PythonOperator(
        task_id="idv_nftf_single_source_task",
        python_callable=adm_idv_aml.task_idv_nftf_single_source,
        op_args=[],
        trigger_rule="all_success"
    )
    idv_nftf_dual_source_task = PythonOperator(
        task_id="idv_nftf_dual_source_task",
        python_callable=adm_idv_aml.task_idv_nftf_dual_source,
        op_args=[],
        trigger_rule="all_success"
    )
    idv_digital_source_task = PythonOperator(
        task_id="idv_digital_source_task",
        python_callable=adm_idv_aml.task_idv_digital_source,
        op_args=[],
        trigger_rule="all_success"
    )

    update_ctl_completed_task = PythonOperator(
        task_id="update_ctl_completed_task",
        python_callable=adm_idv_aml.task_update_ctl_completed,
        op_args=[],
        trigger_rule="all_success"
    )

    update_digital_ctl_process_time_task = PythonOperator(
        task_id="update_digital_ctl_process_time_task",
        python_callable=adm_idv_aml.task_update_digital_ctl_process_time,
        op_args=[],
        trigger_rule="all_success"
    )

    insert_from_all_methods_task = PythonOperator(
        task_id="insert_from_all_methods_task",
        python_callable=adm_idv_aml.task_insert_from_all_methods,
        op_args=[],
        trigger_rule="all_success"
    )

    trigger_cust_idv_dag_task = TriggerDagRunOperator(
        task_id="trigger_cust_idv_dag_task",
        trigger_dag_id="aml_to_cust_idv_info",
        trigger_rule="all_success"
    )

    pipeline_complete = EmptyOperator(
        task_id="pipeline_complete", trigger_rule="all_success")

    pipeline_start >> create_control_table_task >> \
        create_init_stg_ftf_nftf_task >> \
        idv_ftf_task >> \
        idv_nftf_pav_task >> \
        idv_nftf_single_source_task >> \
        idv_nftf_dual_source_task >> \
        idv_digital_source_task >> \
        update_digital_ctl_process_time_task >> \
        update_ctl_completed_task >> \
        insert_from_all_methods_task >> \
        trigger_cust_idv_dag_task >> \
        pipeline_complete
