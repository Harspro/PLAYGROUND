from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.dataproc import (DataprocCreateClusterOperator, DataprocSubmitJobOperator)
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from google.cloud import storage, bigquery

from google.protobuf.duration_pb2 import Duration

from util.miscutils import read_variable_or_file, save_job_to_control_table
import util.constants as consts

import logging
import json
import pendulum

airflow_env_var_dataproc_config = read_variable_or_file('dataproc_config')
airflow_env_var_gcp_config = read_variable_or_file('gcp_config')
deploy_env = airflow_env_var_gcp_config['deployment_environment_name']

dag_config = read_variable_or_file(f"tsys_processing/ods-to-bigquery-connector-{deploy_env}-config", deploy_env)

job_dir = f"{date.today().strftime('%Y%m%d')}"
gcs_staging_dir = f"{dag_config.get('gcs_staging_root_dir')}/{job_dir}"

env_placeholder = '{env}'
local_tz = pendulum.timezone('America/Toronto')


def load_data_for_table(table_name, gcs_table_staging_dir):
    logging.info(f"Starting import of data for table {table_name} from {gcs_table_staging_dir}")
    bq_client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.PARQUET
    )
    bq_table_id = f"{airflow_env_var_gcp_config.get('curated_zone_project_id')}.{dag_config.get('bq_dataset_name')}.{table_name}"
    bq_load_job = bq_client.load_table_from_uri(
        gcs_table_staging_dir,
        bq_table_id,
        job_config=job_config
    )
    bq_load_job.result()

    target_table = bq_client.get_table(bq_table_id)
    logging.info(
        f"Finished import of data for table {table_name} from {gcs_table_staging_dir}. Imported {target_table.num_rows} rows.")


def load_data_for_all_tables():
    logging.info(f"Starting import of data for all tables from {gcs_staging_dir} into BigQuery")

    gcs_bucket_name = dag_config.get('gcs_staging_bucket_name')
    gcs_dir_prefix = gcs_staging_dir[(gcs_staging_dir.index(gcs_bucket_name) + len(gcs_bucket_name) + 1):]

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(gcs_bucket_name)
    blobs_iterator = bucket.list_blobs(prefix=f"{gcs_dir_prefix}/", delimiter="/")
    response = blobs_iterator._get_next_page_response()

    for prefix in response['prefixes']:
        start_index = prefix.index(job_dir) + len(job_dir) + 1
        end_index = prefix.index("/", start_index)
        table_name = prefix[start_index:end_index]

        gcs_table_staging_dir = f"gs://{gcs_bucket_name}/{prefix}*.parquet"

        load_data_for_table(table_name, gcs_table_staging_dir)

    logging.info(f"Finished import of data for all tables from {gcs_staging_dir} into BigQuery")
    return "GCS to BigQuery load task completed for all tables"


def get_dataproc_cluster_config(deploy_env: str, network_tag: str) -> dict:
    subnetwork_uri = dag_config.get('gcp_subnetwork_uri')

    cluster_config_tmpl = {
        "master_config": {
            "num_instances": dag_config.get('dataproc_spark_cluster_master_instances'),
            "machine_type_uri": dag_config.get('dataproc_spark_cluster_master_machine_type'),
            "disk_config": {"boot_disk_type": dag_config.get('dataproc_spark_cluster_master_boot_disk_type'),
                            "boot_disk_size_gb": dag_config.get('dataproc_spark_cluster_master_boot_disk_size_gb')}
        },
        "worker_config": {
            "num_instances": dag_config.get('dataproc_spark_cluster_worker_instances'),
            "machine_type_uri": dag_config.get('dataproc_spark_cluster_worker_machine_type'),
            "disk_config": {"boot_disk_type": dag_config.get('dataproc_spark_cluster_worker_boot_disk_type'),
                            "boot_disk_size_gb": dag_config.get('dataproc_spark_cluster_worker_boot_disk_size_gb')}
        },
        "software_config": {
            "image_version": dag_config.get('dataproc_spark_cluster_image_version'),
            "properties": {
                "dataproc:dataproc.logging.stackdriver.job.driver.enable": dag_config.get('dataproc_logging_stackdriver_job_driver_enable'),
                "dataproc:dataproc.logging.stackdriver.enable": dag_config.get('dataproc_logging_stackdriver_enable'),
                "dataproc:jobs.file-backed-output.enable": dag_config.get('dataproc_jobs_file_backed_output_enable'),
                "dataproc:dataproc.logging.stackdriver.job.yarn.container.enable": dag_config.get('dataproc_logging_stackdriver_job_yarn_container_enable'),
                "dataproc:dataproc.monitoring.stackdriver.enable": dag_config.get('dataproc_monitoring_stackdriver_enable'),
                "spark:spark.dynamicAllocation.enabled": dag_config.get('dataproc_spark_dynamicAllocation_enabled')
            }
        },
        "gce_cluster_config": {
            "service_account": dag_config.get('dataproc_gce_cluster_service_account'),
            "service_account_scopes": [dag_config.get('dataproc_gce_cluster_service_account_scopes')],
            "subnetwork_uri": subnetwork_uri,
            "internal_ip_only": True,
            "tags": [network_tag, dag_config.get('dataproc_gce_cluster_tags')]
        },
        "endpoint_config": {
            "enable_http_port_access": True
        },
    }

    cfg_str = json.dumps(cluster_config_tmpl)

    if deploy_env is not None and env_placeholder in cfg_str:
        cfg_str = cfg_str.replace(env_placeholder, deploy_env)

    cluster_config = json.loads(cfg_str)

    cluster_config["lifecycle_config"] = {
        "idle_delete_ttl": Duration(seconds=dag_config.get('dataproc_spark_cluster_max_idle_limit_sec'))
    }

    return cluster_config


DAG_DEFAULT_ARGS = {
    "owner": "team-defenders-alerts",
    'capability': 'risk-management',
    'severity': 'P3',
    'sub_capability': 'fraud',
    'business_impact': 'Transactions missing enriched data',
    'customer_impact': 'N/A',
    "start_date": datetime(2021, 1, 1),
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
    "wait_for_downstream": False,
    "max_active_runs": 1,
    "retries": 0,
    "retry_delay": timedelta(seconds=10)
}

severity_tag = DAG_DEFAULT_ARGS['severity']


dag = DAG(
    "ods-to-bigquery-connector-dag",
    default_args=DAG_DEFAULT_ARGS,
    description="DAG to orchestrate the tasks to load tables from ODS to BigQuery",
    is_paused_upon_creation=True,
    schedule="0 1 * * *",
    start_date=pendulum.today('America/Toronto').add(days=-2),
    tags=["ods-to-bigquery-connector", severity_tag]
)
dag_owner = DAG_DEFAULT_ARGS['owner']
dag_info = f"[dag_name: {'{{dag.dag_id}}'}, dag_run_id: {'{{run_id}}'}, dag_owner: {dag_owner}]"

SPARK_JOB = {
    "reference": {"project_id": airflow_env_var_dataproc_config.get('project_id')},
    "placement": {"cluster_name": dag_config.get('dataproc_cluster_name')},
    "spark_job": {
        "jar_file_uris": [dag_config.get('dataproc_spark_jar_file_uri')],
        "main_class": dag_config.get('dataproc_spark_main_class_name'),
        "file_uris": [dag_config.get('dataproc_job_conf_file_uri')],
        "properties": {'spark.driver.cores': dag_config.get('dataproc_spark_driver_cores'),
                       'spark.driver.memory': dag_config.get('dataproc_spark_driver_memory'),
                       'spark.executor.instances': dag_config.get('dataproc_spark_num_executors'),
                       'spark.executor.cores': dag_config.get('dataproc_spark_cores_per_executor'),
                       'spark.executor.memory': dag_config.get('dataproc_spark_memory_per_executor'),
                       "spark.sql.legacy.parquet.int96RebaseModeInRead": "CORRECTED",
                       "spark.sql.legacy.parquet.int96RebaseModeInWrite": "CORRECTED",
                       "spark.sql.legacy.parquet.datetimeRebaseModeInRead": "CORRECTED",
                       "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "CORRECTED"},
        "args": [dag_config.get('dataproc_job_arg_conf_file_name'), gcs_staging_dir, f'{consts.SPARK_DAG_INFO}={dag_info}']
    }
}

create_dataproc_cluster_task = DataprocCreateClusterOperator(
    task_id="create_dataproc_cluster_task",
    project_id=airflow_env_var_gcp_config.get("processing_zone_project_id"),
    cluster_config=get_dataproc_cluster_config(deploy_env, airflow_env_var_gcp_config.get("network_tag")),
    region=airflow_env_var_dataproc_config.get('location'),
    cluster_name=dag_config.get('dataproc_cluster_name'),
    dag=dag
)

submit_dataproc_job_task = DataprocSubmitJobOperator(
    task_id="dataproc_spark_submit_task",
    job=SPARK_JOB,
    project_id=airflow_env_var_dataproc_config.get('project_id'),
    region=airflow_env_var_dataproc_config.get('location'),
    gcp_conn_id=airflow_env_var_gcp_config.get('processing_zone_connection_id'),
    asynchronous=False,
    execution_timeout=timedelta(minutes=480),
    dag=dag
)

bulk_load_all_bq_tables_task = PythonOperator(
    task_id="gcs_to_bq_bulk_loader_task",
    python_callable=load_data_for_all_tables,
    execution_timeout=timedelta(minutes=120),
    dag=dag
)

job_params_str = json.dumps({})
save_dag_to_bq_task = PythonOperator(
    task_id='save_job_to_control_table',
    python_callable=save_job_to_control_table,
    op_kwargs={"job_params": job_params_str},
    dag=dag
)

end_task = EmptyOperator(
    task_id="Done",
    dag=dag
)

create_dataproc_cluster_task >> submit_dataproc_job_task >> bulk_load_all_bq_tables_task >> save_dag_to_bq_task >> end_task
