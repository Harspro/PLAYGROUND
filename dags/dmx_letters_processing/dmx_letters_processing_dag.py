import logging
import re
from datetime import timedelta, datetime
from typing import Final

from airflow import DAG, settings
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator, DataprocCreateClusterOperator
from airflow.utils.task_group import TaskGroup
from dag_factory.abc import BaseDagBuilder
from util.deploy_utils import read_pause_unpause_setting, pause_unpause_dag
from util.logging_utils import build_spark_logging_info
from util.miscutils import get_cluster_name_for_dag, \
    get_cluster_config_by_job_size
from util.bq_utils import create_external_table, apply_column_transformation, run_bq_query
from google.cloud import bigquery

import util.constants as consts

from dag_factory.terminus_dag_factory import DAGFactory

logger = logging.getLogger(__name__)

DMX_LETTERS_CONFIG_PATH: Final[str] = f'{settings.DAGS_FOLDER}/dmx_letters_processing'
DMX_LETTERS_CONFIG_FILE: Final[str] = 'dmx_letters_config.yaml'
EST_ZONE: Final[str] = 'America/Toronto'


class DmxLettersDagBuilder(BaseDagBuilder):
    """
    DMX Letters Processing DAG Builder

    A specialized DAG builder for processing DMX RRDF files
    into the BigQuery MAIL_PRINT table. This class orchestrates an end-to-end data pipeline
    that includes Spark-based data transformation and BigQuery loading operations.

    ## Purpose
    The DmxLettersDagBuilder processes DMX RRDF files containing letter printing data for
    customer communications. It transforms JSON-formatted input files into structured
    Parquet files and loads them into the BigQuery MAIL_PRINT table for downstream
    processing by printing and mailing systems.

    ## Data Flow
    Input: DMX RRDF JSON files → Spark Processing → Parquet files → BigQuery MAIL_PRINT table

    ## Related dags
    - doc_generation_processing (downstream letter printing)
    """

    def create_unique_folder_path(self, **kwargs):
        """
            Generate a unique folder path based on the provided DAG run ID.

            This method ensures that each DAG run writes its Parquet files to a distinct
            directory by appending the DAG run ID to the base path. Using a unique
            folder path prevents file overwrites and keeps outputs isolated per run.
        """
        dag_run_id = kwargs['dag_run'].run_id
        unique_folder_path = re.sub(r'[^a-zA-Z0-9]', '_', dag_run_id)
        return unique_folder_path

    def load_parquet_to_bq(self, bq_config: dict, unique_folder_path: str):
        """
            Loads Parquet files from a unique GCS folder into a BigQuery table.
            Creates an external table, applies optional column transformations,
            and inserts the transformed data into the destination table.
        """
        output_gcs_prefix = bq_config.get('output_gcs_prefix')
        gcs_table_staging_dir = f"{output_gcs_prefix}/{unique_folder_path}/*.parquet"

        project_id = bq_config.get(consts.PROJECT_ID)
        dataset_name = bq_config.get(consts.DATASET_ID)
        table_name = bq_config.get(consts.TABLE_NAME)

        bq_table_id = f"{project_id}.{dataset_name}.{table_name}"
        bq_ext_table_id = f"{self.environment_config.gcp_config.get(consts.PROCESSING_ZONE_PROJECT_ID)}.{dataset_name}.{table_name}_EXT"

        try:
            bq_client = bigquery.Client()

            # Create external table from parquet files
            transformed_data = create_external_table(bq_client, bq_ext_table_id, gcs_table_staging_dir)

            # Apply column transformations (add and drop columns)
            add_columns = bq_config.get(consts.ADD_COLUMNS) or []
            drop_columns = bq_config.get(consts.DROP_COLUMNS) or []
            if add_columns or drop_columns:
                transformed_data = apply_column_transformation(bq_client, transformed_data, add_columns, drop_columns)
                logger.info(f"Applying column transformations: add={add_columns}, drop={drop_columns}")

            columns = transformed_data.get(consts.COLUMNS)

            dest_table_ddl = f"""
                INSERT INTO `{bq_table_id}` ({columns})
                SELECT {columns}
                FROM `{transformed_data.get(consts.ID)}`;
            """

            logging.info(f"Executing BigQuery DDL: {dest_table_ddl}")
            run_bq_query(dest_table_ddl)

            target_table = bq_client.get_table(bq_table_id)
            logging.info(f"Successfully loaded parquet data to {bq_table_id}. Table now has {target_table.num_rows} rows.")

        except Exception as err:
            logger.error(f"Error loading parquet data to BigQuery: {err}", exc_info=True, stack_info=True)
            raise AirflowFailException(f"Failed to load parquet data to BigQuery: {str(err)}") from None

    def build_spark_job(self, cluster_name: str, input_bucket_name: str, input_folder_name: str, input_file_name: str,
                        unique_folder_path: str, spark_config: dict):
        """
            Builds and returns a Spark job configuration for Dataproc.
            It sets up input/output paths in GCS, applies additional Spark arguments,
            and prepares the job for execution on the specified cluster.
        """
        input_file_path = f"gs://{input_bucket_name}/{input_folder_name}/{input_file_name}"
        output_file_path = f"gs://pcb-{self.environment_config.deploy_env}-staging-extract/dmx_letters/{unique_folder_path}"
        arg_list = [f'input.file.path={input_file_path}',
                    f'output.file.path={output_file_path}']

        dataproc_job_args = spark_config.get(consts.ARGS)
        for k, v in dataproc_job_args.items():
            if k not in ['input.file.path', 'output.file.path']:
                arg_list.append(f'{k}={v}')

        arg_list = build_spark_logging_info(dag_id='{{dag.dag_id}}', default_args=self.default_args, arg_list=arg_list)

        spark_job = {
            consts.REFERENCE: {consts.PROJECT_ID: self.environment_config.dataproc_config.get(consts.PROJECT_ID)},
            consts.PLACEMENT: {consts.CLUSTER_NAME: cluster_name},
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: spark_config[consts.JAR_FILE_URIS],
                consts.MAIN_CLASS: spark_config[consts.MAIN_CLASS],
                consts.ARGS: arg_list
            }
        }

        return spark_job

    def build(self, dag_id: str, config: dict) -> DAG:
        """
            Builds and returns an Airflow DAG that processes DMX RRDF files end-to-end.

            This DAG orchestrates:
              • **EmptyOperator (start/end):** Marks the start and end of the workflow.
              • **PythonOperator – create_unique_folder_path:** Generates a unique folder path per DAG run.
              • **DataprocCreateClusterOperator:** Creates a temporary Dataproc cluster for Spark processing.
              • **DataprocSubmitJobOperator:** Submits the Spark job to parse files and generate Parquet outputs.
              • **PythonOperator – load_parquet_to_bq:** Loads the generated Parquet files from GCS into BigQuery.

            Together these tasks handle cluster setup, data transformation, and loading results into BigQuery.
        """
        self.default_args = self.prepare_default_args(
            config_args=config.get(consts.DEFAULT_ARGS, {})
        )
        with DAG(
                dag_id=dag_id,
                default_args=self.default_args,
                description=(
                    'This DAG processes DMX RRDF files by loading JSON data, mapping it, '
                    'and writing the results to the MAIL_PRINT table in BigQuery using a Dataproc cluster.'
                ),
                render_template_as_native_obj=True,
                schedule=None,
                start_date=datetime(2025, 9, 1, tzinfo=self.environment_config.local_tz),
                catchup=False,
                max_active_tasks=5,
                max_active_runs=1,
                dagrun_timeout=timedelta(hours=6),
                tags=config[consts.TAGS],
                is_paused_upon_creation=True
        ) as dag:
            dag_config = config.get(consts.DAG)
            if dag_config.get(consts.READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(dag_id, self.environment_config.deploy_env)
                pause_unpause_dag(dag, is_paused)

            start = EmptyOperator(task_id=consts.START_TASK_ID)

            create_unique_folder_path_task = PythonOperator(
                task_id="create_unique_folder_path",
                python_callable=self.create_unique_folder_path
            )
            cluster_name = get_cluster_name_for_dag(dag_id)
            xcom_unique_folder_path = "{{ task_instance.xcom_pull(task_ids='create_unique_folder_path', key='return_value') }}"

            with TaskGroup(group_id="dataproc_processing") as task_group:
                cluster_creating_task = DataprocCreateClusterOperator(
                    task_id=consts.CLUSTER_CREATING_TASK_ID,
                    project_id=self.environment_config.dataproc_config.get(consts.PROJECT_ID),
                    cluster_config=get_cluster_config_by_job_size(self.environment_config.deploy_env,
                                                                  self.environment_config.gcp_config.get(
                                                                      consts.NETWORK_TAG)),
                    region=self.environment_config.dataproc_config.get(consts.LOCATION),
                    cluster_name=cluster_name
                )

                dataproc_task = DataprocSubmitJobOperator(
                    task_id="parse_file_and_generate_parquet",
                    job=self.build_spark_job(
                        cluster_name,
                        "{{ dag_run.conf['bucket'] }}",
                        "{{ dag_run.conf['folder_name'] }}",
                        "{{ dag_run.conf['file_name'] }}",
                        xcom_unique_folder_path,
                        dag_config.get(consts.SPARK)
                    ),
                    region=self.environment_config.dataproc_config.get(consts.LOCATION),
                    project_id=self.environment_config.dataproc_config.get(consts.PROJECT_ID),
                    gcp_conn_id=self.environment_config.gcp_config.get(
                        consts.PROCESSING_ZONE_CONNECTION_ID
                    )
                )

                cluster_creating_task >> dataproc_task

            load_parquet_to_bq_task = PythonOperator(
                task_id="load_parquet_to_bq",
                python_callable=self.load_parquet_to_bq,
                op_kwargs={
                    'bq_config': dag_config.get(consts.BIGQUERY),
                    'unique_folder_path': xcom_unique_folder_path
                }
            )

            end = EmptyOperator(task_id=consts.END_TASK_ID)

            start >> create_unique_folder_path_task >> task_group >> load_parquet_to_bq_task >> end

            return dag


globals().update(DAGFactory().create_dynamic_dags(DmxLettersDagBuilder, DMX_LETTERS_CONFIG_FILE, DMX_LETTERS_CONFIG_PATH))
