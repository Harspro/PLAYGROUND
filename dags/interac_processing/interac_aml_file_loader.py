import os
import logging
from datetime import datetime, timedelta

from airflow import DAG, settings
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator
)

import util.constants as consts
from util.miscutils import read_variable_or_file, get_serverless_cluster_config
from util.logging_utils import build_spark_logging_info
from dag_factory.abc import BaseDagBuilder
from dag_factory.environment_config import EnvironmentConfig
from dag_factory.terminus_dag_factory import DAGFactory

logger = logging.getLogger(__name__)


class InteracAmlDagBuilder(BaseDagBuilder):
    """
    Interac AML File Loader DAG - Spark writes JSONL, BigQuery loads directly.
    """

    def __init__(self, environment_config: EnvironmentConfig):
        super().__init__(environment_config)
        self.local_tz = environment_config.local_tz
        self.gcp_config = environment_config.gcp_config
        self.deploy_env = environment_config.deploy_env
        self.landing_project_id = self.gcp_config.get(consts.LANDING_ZONE_PROJECT_ID)
        self.processing_project_id = self.gcp_config.get(consts.PROCESSING_ZONE_PROJECT_ID)
        self.dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)

    def submit_json_conversion_job(self, config: dict):
        """Build Dataproc Serverless Batch task for Spark JAR."""
        spark_config = config.get("spark", {})
        processing_bucket = config.get("processing_bucket", f"pcb-{self.deploy_env}-staging")
        processing_bucket_extract = config.get("processing_bucket_extract", f"pcb-{self.deploy_env}-staging-extract")
        arglist = [
            f'pcb.interac.aml.processor.datafile.path=gs://{processing_bucket}/{{{{ dag_run.conf["name"] }}}}',
            f'pcb.interac.aml.processor.output.path=gs://{processing_bucket_extract}/interac_aml_jsonl/{{{{ dag_run.conf["name"] | replace(".json", "") }}}}/'
        ]
        default_args = {
            "owner": "team-money-movement-eng",
            "capability": "Payments",
            "severity": "P3",
            "sub_capability": "EMT"
        }
        arglist = build_spark_logging_info(
            dag_id='{{dag.dag_id}}',
            default_args=default_args,
            arg_list=arglist
        )
        subnetwork_uri, network_tags, service_account = get_serverless_cluster_config(
            self.deploy_env,
            self.gcp_config.get(consts.NETWORK_TAG)
        )

        spark_batch_config = {
            "main_class": spark_config.get("main_class"),
            "jar_file_uris": spark_config.get("jar_file_uris", []),
            "args": arglist
        }

        file_uris = spark_config.get("file_uris")
        if file_uris is not None and file_uris:
            spark_batch_config["file_uris"] = file_uris

        batch_config = {
            "spark_batch": spark_batch_config,
            "environment_config": {
                "execution_config": {
                    "subnetwork_uri": subnetwork_uri,
                    "network_tags": network_tags,
                    "service_account": service_account
                }
            }
        }

        properties = spark_config.get("properties")
        if properties is not None and properties:
            properties_str = {k: str(v) for k, v in properties.items()}
            batch_config["runtime_config"] = {
                "properties": properties_str
            }
        batch_id = "{{ (dag.dag_id.replace('_', '-') + '-' + ts_nodash).lower() }}"

        return DataprocCreateBatchOperator(
            task_id=consts.CLUSTER_CREATING_TASK_ID,
            batch=batch_config,
            region=self.dataproc_config.get(consts.LOCATION),
            project_id=self.dataproc_config.get(consts.PROJECT_ID),
            gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID),
            batch_id=batch_id
        )

    def load_jsonl_to_bigquery(self, **context):
        """
        Load JSONL files from GCS to BigQuery using BigQuery client.
        """
        from google.cloud import bigquery
        from google.cloud import storage

        config = context['config']
        processing_bucket_extract = config.get("processing_bucket_extract", f"pcb-{self.deploy_env}-staging-extract")
        bq_dataset_name = config.get("bq_dataset_id")
        bq_table_name = config.get(consts.TABLE_NAME)
        file_name = context['dag_run'].conf['name']

        file_name_base = file_name.replace('.json', '')
        jsonl_path = f"gs://{processing_bucket_extract}/interac_aml_jsonl/{file_name_base}/"
        final_table_id = f"{self.landing_project_id}.{bq_dataset_name}.{bq_table_name}"

        logger.info(f"Looking for JSONL files in: {jsonl_path}")

        storage_client = storage.Client()
        bucket_name = processing_bucket_extract
        prefix = f"interac_aml_jsonl/{file_name_base}/"

        bucket = storage_client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=prefix))

        if not blobs:
            raise ValueError(f"No files found at gs://{bucket_name}/{prefix}")

        logger.info(f"Found {len(blobs)} files")

        jsonl_files = [blob.name for blob in blobs if blob.name.endswith('.json')]

        if not jsonl_files:
            raise ValueError(f"No .json files found in {jsonl_path}")

        gcs_uri = f"gs://{bucket_name}/{jsonl_files[0]}"
        logger.info(f"Loading JSONL from: {gcs_uri}")

        # Load to BigQuery
        client = bigquery.Client()
        final_table = client.get_table(final_table_id)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=final_table.schema,
            ignore_unknown_values=True,
            max_bad_records=0
        )

        load_job = client.load_table_from_uri(gcs_uri, final_table_id, job_config=job_config)
        load_job.result()

        rows_loaded = load_job.output_rows or 0
        logger.info(f"Successfully loaded {rows_loaded} rows to {final_table_id}")

    def build(self, dag_id: str, config: dict) -> DAG:
        default_args = self.prepare_default_args({
            "owner": "team-money-movement-eng",
            "capability": "Payments",
            "severity": "P3",
            "sub_capability": "EMT",
            "business_impact": "AML files from Interac are not processed and stored",
            "customer_impact": "AML compliance data not available for analysis and reporting",
            "retries": 3
        })

        dag = DAG(
            dag_id=dag_id,
            schedule=None,
            start_date=datetime(2026, 1, 1, tzinfo=self.local_tz),
            max_active_runs=1,
            is_paused_upon_creation=True,
            catchup=False,
            default_args=default_args
        )

        with dag:
            start = EmptyOperator(task_id=consts.START_TASK_ID)

            # Copy file from landing zone to processing bucket
            copy_file = GCSToGCSOperator(
                task_id="copy_file_to_processing",
                source_bucket="{{ dag_run.conf['bucket'] }}",
                source_object="{{ dag_run.conf['name'] }}",
                destination_bucket=config.get("processing_bucket", f"pcb-{self.deploy_env}-staging"),
                destination_object="{{ dag_run.conf['name'] }}",
                gcp_conn_id=self.gcp_config.get(consts.LANDING_ZONE_CONNECTION_ID)
            )

            # Run Spark job (writes JSONL)
            convert_json_to_jsonl = self.submit_json_conversion_job(config)

            # Load JSONL to BigQuery
            load_jsonl = PythonOperator(
                task_id="load_jsonl_to_bigquery",
                python_callable=self.load_jsonl_to_bigquery,
                op_kwargs={'config': config}
            )

            end = EmptyOperator(task_id=consts.END_TASK_ID)

            start >> copy_file >> convert_json_to_jsonl >> load_jsonl >> end

        return dag


globals().update(DAGFactory().create_dynamic_dags(
    InteracAmlDagBuilder,
    config_filename='interac_aml_file_loader_config.yaml',
    config_dir=f'{settings.DAGS_FOLDER}/interac_processing'
))
