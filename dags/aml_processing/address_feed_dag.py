from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from aml_processing import feed_commons as commons
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

FEED_CONFIG = commons.read_feed_config('feed.address', 'aml_processing', 'aml_dag_config.yaml', )
DAG_NAME = "aml_address_feed"
START_DATE = datetime(2022, 1, 1, tzinfo=commons.TORONTO_TZ)  # days_ago(1).replace(tzinfo=LOCAL_TZ)

# Modify the default_args dictionary
custom_default_args = commons.DAG_DEFAULT_ARGS.copy()
custom_default_args['business_impact'] = 'Customer address information in Verafin will not be up-to-date if this process fails'
tags = commons.TAGS.copy()
address_tag = custom_default_args['severity']
tags.append(address_tag)

with DAG(
    DAG_NAME,
    default_args=custom_default_args,
    description="DAG to orchestrate the AML Address Feed. Puts the output file in the outbound bucket",
    schedule='0 9 * * 1-5',
    start_date=START_DATE,
    tags=tags,
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=True
) as dag:

    pipeline_start = EmptyOperator(task_id="pipeline_start")

    run_address_idl_to_bq = PythonOperator(
        task_id="run_address_idl_to_bq",
        python_callable=commons.run_feed_idl,
        op_args=[FEED_CONFIG[commons.SQL_FILE_PATH], FEED_CONFIG[commons.DESTINATION_BQ_DATASET],
                 FEED_CONFIG[commons.BQ_TABLE_NAME]],
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    export_table_to_gcs = PythonOperator(
        task_id="export_table_to_gcs",
        python_callable=commons.export_bq_table,
        op_args=[FEED_CONFIG[commons.STAGING_BUCKET], FEED_CONFIG[commons.BQ_TO_GCS_FOLDER],
                 FEED_CONFIG[commons.DESTINATION_BQ_DATASET], FEED_CONFIG[commons.BQ_TABLE_NAME], False, True],
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    compose_feed_file = PythonOperator(
        task_id="compose_feed_file",
        python_callable=commons.compose_infinite_files_into_one,
        op_args=[FEED_CONFIG[commons.STAGING_BUCKET], FEED_CONFIG[commons.STAGING_FOLDER],
                 FEED_CONFIG[commons.SOURCE_HEADERS_FILE_PATH], FEED_CONFIG[commons.DESTINATION_HEADERS_FILE_PATH],
                 FEED_CONFIG[commons.BQ_TO_GCS_FOLDER], FEED_CONFIG[commons.BQ_TABLE_NAME]],
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    copy_object_to_outbound_bucket_task = PythonOperator(
        task_id="copy_object_to_outbound_bucket_task",
        python_callable=commons.copy_object,
        op_args=[FEED_CONFIG[commons.STAGING_BUCKET], FEED_CONFIG[commons.STAGING_FOLDER],
                 FEED_CONFIG[commons.OUTBOUND_BUCKET], FEED_CONFIG[commons.OUTBOUND_FILENAME]],
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    collect_metrics = PythonOperator(
        task_id="collect_metrics",
        python_callable=commons.collect_metrics,
        op_args=[DAG_NAME, FEED_CONFIG[commons.MONITORING_BQ_TABLE], FEED_CONFIG[commons.MONITORING_BQ_DATASET],
                 FEED_CONFIG[commons.MONITORING_BQ_TABLE_SCHEMA_FILE_PATH]],
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE
    )

    final_task = PythonOperator(
        task_id="final_task",
        python_callable=commons.final_task,
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag
    )

    pipeline_start >> run_address_idl_to_bq >> \
        export_table_to_gcs >> \
        compose_feed_file >> \
        copy_object_to_outbound_bucket_task >> \
        collect_metrics >> \
        final_task


# rundown of basic dag steps
# 1. run idl -> will read sql file from gcs and run in bq; bq query settings should save table
# 2. export table -> use api to export table data (without headers and use wildcard URI so it will save into multiple files if larger than 1 GB) into a file into extract folder
#                 -> should probably also add a step or substep to check if files exist within extract folder, if so delete all files (including headers file) within folder before exporting
# 3. compose file file within extract folder -> copy headers files from dag subfolder to extract folder, use storage python api to compose blobs of data files and header file together
# 4. copy final file to outbound landing bucket and rename appropriately
