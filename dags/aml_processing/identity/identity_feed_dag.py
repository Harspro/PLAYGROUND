from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from aml_processing import feed_commons as commons
from aml_processing.identity.identity_extraction import get_identity_feed_extract_sql

############################################################
# Configs and constants
############################################################

FEED_CONFIG = commons.read_feed_config('feed.identity', 'aml_processing', 'aml_dag_config.yaml', )
DAG_NAME = "aml_identity_feed"
START_DATE = datetime(2022, 1, 1, tzinfo=commons.TORONTO_TZ)
tags = commons.TAGS.copy()
tags.append("identity")
IDENTITY_EXTRACTION_QUERY = get_identity_feed_extract_sql()

# Modify the default_args dictionary
custom_default_args = commons.DAG_DEFAULT_ARGS.copy()
custom_default_args['business_impact'] = 'Customer identity information in Verafin will not be up-to-date if this process fails'
tags.append(custom_default_args['severity'])

with DAG(
    DAG_NAME,
    default_args=custom_default_args,
    description="DAG to orchestrate the AML Identity Feed. Puts the output file in the outbound bucket",
    schedule='30 7 * * 1-5',
    start_date=START_DATE,
    tags=tags,
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=True
) as dag:

    pipeline_start = EmptyOperator(task_id="pipeline_start")

    run_identity_idl_to_bq = PythonOperator(
        task_id="run_identity_idl_to_bq",
        python_callable=commons.run_feed_idl_with_query,
        op_args=[IDENTITY_EXTRACTION_QUERY, FEED_CONFIG[commons.DESTINATION_BQ_DATASET],
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

    pipeline_start >> run_identity_idl_to_bq >> \
        export_table_to_gcs >> \
        compose_feed_file >> \
        copy_object_to_outbound_bucket_task >> \
        collect_metrics >> \
        final_task
