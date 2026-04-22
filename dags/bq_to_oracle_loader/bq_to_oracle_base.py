import logging
import pendulum
from airflow.exceptions import AirflowFailException, AirflowException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from google.cloud import storage
from airflow import settings
from airflow.models import Variable
from google.cloud import bigquery
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from copy import deepcopy
from typing import Final, Union
import os
from util.deploy_utils import read_pause_unpause_setting, pause_unpause_dag
import bq_to_oracle_loader.utils.constants as constants
import util.constants as consts
from util.bq_utils import read_sql_file
from etl_framework.etl_dag_base import ETLDagBase
from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from util.miscutils import (
    read_variable_or_file,
    save_job_to_control_table,
    get_cluster_config_by_job_size, read_file_env, read_env_filepattern)
from util.gcs_utils import compose_file, list_blobs_with_prefix, delete_blobs, upload_blob, delete_folder
import util.logging_utils as logging_utils
import util.bq_utils as bq_utils
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)
SQL_FILE_PATH = f'{settings.DAGS_FOLDER}/bq_to_oracle_loader/sql/'
UPDATE_JOB_ARGS: Final = 'updater_job_args'
UPDATE_JAR_URI: Final = 'update_jar_uri'
UPDATER_MAIN_CLASS: Final = 'updater_main_class'
local_tz = pendulum.timezone('America/Toronto')
CURRENT_DATETIME = datetime.now(tz=local_tz)
JOB_DATE_TIME = f"{CURRENT_DATETIME.strftime('%Y%m%d%H%M%S')}"


class BqToOracleProcessor(ETLDagBase):
    def __init__(self, module_name, config_path, config_filename, dag_default_args):
        super().__init__(module_name, config_path, config_filename, dag_default_args)
        self.default_args = deepcopy(dag_default_args)
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)

    def preprocessing_job(self, dag_config: dict, upstream_task: list):
        pass

    def create_temp_tables(self, dag_config: dict, file_create_dt: str, segment_name: str, file_name: str):
        logger.info(f"Received dag_config: {dag_config}")
        logger.info(f"file create date for this execution is: {file_create_dt}")
        logger.info(f" table name for this execution is: {segment_name}")
        if not segment_name:
            raise ValueError("Missing 'segment_name' in dag_config.")
        output_table_name = f'{segment_name}_{constants.TEMP_TABLE_SUFFIX}'
        logger.info(f"output table name for this execution is: {output_table_name}")
        sql_file = f"{output_table_name}.sql"
        sql_full_path = os.path.join(SQL_FILE_PATH, sql_file)
        logger.info(f"SQL file for the task is: {sql_full_path}")
        try:
            create_table_sql = read_file_env(sql_full_path, self.deploy_env)
            file_name = file_name.split('/')[-1]
            create_table_sql = create_table_sql.replace(constants.FILE_NAME_PLACEHOLDER, file_name)
            result = bq_utils.run_bq_query(
                create_table_sql.replace(constants.FILE_CREATE_DT_PLACEHOLDER, file_create_dt))
            logger.info(f"Data load in temp table status:{result}")
        except Exception as e:
            logger.error(f"Error during data load: {e}")
            raise AirflowFailException(f"Data load job failed: {e}")

    def build_cluster_creating_task(self, job_size: str, cluster_name: str):
        return DataprocCreateClusterOperator(
            task_id=consts.CLUSTER_CREATING_TASK_ID,
            project_id=self.dataproc_config.get(consts.PROJECT_ID),
            cluster_config=get_cluster_config_by_job_size(self.deploy_env, self.gcp_config.get(consts.NETWORK_TAG),
                                                          job_size),
            region=self.dataproc_config.get(consts.LOCATION),
            cluster_name=cluster_name
        )

    def get_oracle_table_segment_list(self, dag_config: dict):
        spark_config = dag_config.get(consts.SPARK)
        segment_configs = spark_config.get(UPDATE_JOB_ARGS)

        return [segment_name for segment_name in segment_configs]

    def export_bq_table_to_gcs(self, dag_config: dict, segment_name: str, **context):
        """
        Export BigQuery table to GCS as CSV/Parquet/Avro files
        """
        logger.info(f"Starting BigQuery to GCS export for segment: {segment_name}")

        bq_config = dag_config.get(consts.BIGQUERY)
        project_id = bq_config.get('processing_project_id')
        dataset_id = bq_config.get(constants.DATASET)
        table_name = f'{segment_name}_{constants.TEMP_TABLE_SUFFIX}'

        export_config = dag_config.get('gcs_export', {})
        bucket_name = export_config.get('bucket_name', f'pcb-{self.deploy_env}-staging')
        file_format = export_config.get('file_format', 'CSV')
        compression = export_config.get('compression', 'GZIP')
        field_delimiter = export_config.get('field_delimiter', '|')

        segment_file_configs = export_config.get('segment_file_names', {})
        segment_file_config = segment_file_configs.get(segment_name, {})

        if segment_file_config:
            gcs_path = segment_file_config.get('gcs_path', 'bq_exports')
        else:
            gcs_path = 'bq_exports'

        logger.info(f"Cleaning up existing files in gs://{bucket_name}/{gcs_path}/")
        delete_folder(bucket_name, gcs_path)
        logger.info(f"Cleanup completed. Folder gs://{bucket_name}/{gcs_path}/ is now empty.")

        if segment_file_config:
            output_filename = segment_file_config.get('output_filename', segment_name)
            filename_ext = segment_file_config.get('file_extension')
            if filename_ext:
                file_extension = read_env_filepattern(filename_ext, self.deploy_env)
            else:
                file_extension = file_format.lower()
        else:
            output_filename = segment_name
            file_extension = file_format.lower()

        output_file_name = f"{output_filename}_{JOB_DATE_TIME}"
        gcs_uri = f"gs://{bucket_name}/{gcs_path}/{output_file_name}-*.{file_extension}"

        logger.info(f"Exporting table {project_id}.{dataset_id}.{table_name} to {gcs_uri}")
        logger.info(f"Export format: {file_format}, Compression: {compression}, Field delimiter: '{field_delimiter}'")

        try:
            client = bigquery.Client(project=project_id)
            table_ref = f"{project_id}.{dataset_id}.{table_name}"

            job_config = bigquery.ExtractJobConfig()
            job_config.compression = getattr(bigquery.Compression, compression)

            if file_format == 'CSV':
                job_config.destination_format = bigquery.DestinationFormat.CSV
                include_header = export_config.get('include_header', True)
                job_config.print_header = include_header
                job_config.field_delimiter = field_delimiter
            elif file_format == 'PARQUET':
                job_config.destination_format = bigquery.DestinationFormat.PARQUET
            elif file_format == 'AVRO':
                job_config.destination_format = bigquery.DestinationFormat.AVRO
            elif file_format == 'JSON':
                job_config.destination_format = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON

            extract_job = client.extract_table(
                table_ref,
                gcs_uri,
                job_config=job_config,
                location=bq_config.get('location', 'northamerica-northeast1')
            )

            extract_job.result()

            logger.info(f"Export completed successfully. Files exported to: {gcs_uri}")

            context['ti'].xcom_push(key=f'{segment_name}_gcs_bucket', value=bucket_name)
            context['ti'].xcom_push(key=f'{segment_name}_gcs_path', value=f"{gcs_path}/{output_file_name}-*")
            context['ti'].xcom_push(key=f'{segment_name}_output_filename', value=f"{output_file_name}-*")

            return gcs_uri

        except Exception as e:
            logger.error(f"Error during BigQuery to GCS export: {e}")
            raise AirflowFailException(f"BigQuery to GCS export failed: {e}")

    def compose_sharded_files_into_one(self, dag_config: dict, segment_name: str, **context):
        """
        Compose multiple sharded files into one file using the feed_commons pattern.
        Handles any number of files by composing in chunks of 32 (GCS compose limit).
        Adds header row from BigQuery table schema at the beginning of the merged file.
        """
        ti = context['ti']
        bucket_name = ti.xcom_pull(task_ids=f'{segment_name}.export_{segment_name}_to_gcs', key=f'{segment_name}_gcs_bucket')
        gcs_path_pattern = ti.xcom_pull(task_ids=f'{segment_name}.export_{segment_name}_to_gcs', key=f'{segment_name}_gcs_path')

        export_config = dag_config.get('gcs_export', {})
        segment_file_configs = export_config.get('segment_file_names', {})
        segment_file_config = segment_file_configs.get(segment_name, {})

        gcs_path_prefix = gcs_path_pattern.replace('-*', '-')

        logger.info(f"Starting compose for segment {segment_name}")
        logger.info(f"Bucket: {bucket_name}, Path prefix: {gcs_path_prefix}")

        chunk_size = 32

        source_blob_list = list_blobs_with_prefix(bucket_name, gcs_path_prefix, delimiter=None)

        if not source_blob_list:
            raise AirflowFailException(f"No files found to compose at {bucket_name}/{gcs_path_prefix}")

        logger.info(f"Found {len(source_blob_list)} sharded files to merge")

        chunk_iteration = 1
        while len(source_blob_list) > chunk_size:
            source_blob_chunk = source_blob_list[:chunk_size]
            logger.info(f"Composing chunk {chunk_iteration}: {len(source_blob_chunk)} files")

            temp_destination_file = f"{gcs_path_prefix}{chunk_iteration:012d}_composed"
            compose_file(bucket_name, source_blob_chunk, temp_destination_file)

            delete_blobs(bucket_name, source_blob_chunk)

            source_blob_list = list_blobs_with_prefix(bucket_name, gcs_path_prefix, delimiter=None)
            logger.info(f"Files remaining after chunk {chunk_iteration}: {len(source_blob_list)}")
            chunk_iteration += 1

        merge_config = export_config.get('merge_sharded_files', {})
        add_header = merge_config.get('add_header', True)

        if add_header:
            header_source_path = segment_file_config.get('header_file')

            if not header_source_path:
                raise AirflowFailException(
                    f"Header file path is required for segment '{segment_name}'. "
                    f"Please specify 'header_file' in segment_file_names.{segment_name} configuration."
                )

            if not header_source_path.startswith('/'):
                header_full_path = f"{settings.DAGS_FOLDER}/{header_source_path}"
            else:
                header_full_path = header_source_path

            if not os.path.exists(header_full_path):
                raise AirflowFailException(f"Header file not found at: {header_full_path}")

            logger.info(f"Using header file: {header_full_path}")

            gcs_path = segment_file_config.get('gcs_path', 'bq_exports')
            header_destination = f"{gcs_path}/{segment_name}_header_{JOB_DATE_TIME}.csv"
            logger.info(f"Uploading header to gs://{bucket_name}/{header_destination}")
            upload_blob(bucket_name, header_full_path, header_destination)

            source_blob_list = [header_destination] + source_blob_list
            logger.info(f"Header file added. Total files to compose: {len(source_blob_list)}")

        if len(source_blob_list) > chunk_size:
            logger.info(f"Total files ({len(source_blob_list)}) exceeds limit ({chunk_size}). Composing one more chunk.")
            source_blob_chunk = source_blob_list[:chunk_size]
            temp_destination_file = f"{gcs_path_prefix}000000000000_header_composed"
            compose_file(bucket_name, source_blob_chunk, temp_destination_file)
            delete_blobs(bucket_name, source_blob_chunk)
            source_blob_list = [temp_destination_file] + source_blob_list[chunk_size:]
            logger.info(f"Files after final chunk: {len(source_blob_list)}")

        output_filename = segment_file_config.get('output_filename', segment_name)
        filename_ext = segment_file_config.get('file_extension')
        if filename_ext:
            file_extension = read_env_filepattern(filename_ext, self.deploy_env)
        else:
            file_extension = export_config.get('file_format', 'CSV').lower()

        gcs_path = segment_file_config.get('gcs_path', 'bq_exports')
        final_destination = f"{gcs_path}/{output_filename}_{JOB_DATE_TIME}_merged.{file_extension}"

        logger.info(f"Final compose: {len(source_blob_list)} files into {final_destination}")
        compose_file(bucket_name, source_blob_list, final_destination)

        logger.info(f"Cleaning up {len(source_blob_list)} source files after compose")
        delete_blobs(bucket_name, source_blob_list)
        logger.info("Source files deleted successfully")

        ti.xcom_push(key=f'{segment_name}_gcs_bucket', value=bucket_name)
        ti.xcom_push(key=f'{segment_name}_gcs_path', value=final_destination)
        ti.xcom_push(key=f'{segment_name}_output_filename', value=f"{output_filename}_{JOB_DATE_TIME}_merged.{file_extension}")

        logger.info(f"Successfully merged files into one: {final_destination}")

    def oracle_table_updates(self, cluster_name: str, dag_config: dict, segment_name: str, **context):
        """
            Initializing spark for loading data from BQ to Oracle Tables
            """
        arglist = []
        spark_config = dag_config.get(consts.SPARK)
        segment_configs = spark_config.get(UPDATE_JOB_ARGS)
        updater_job_args = segment_configs.get(segment_name)
        arglist.append(spark_config.get("application.config"))
        arglist.append(updater_job_args.get("oracle.schema.table.name"))
        if updater_job_args.get("operation.type"):
            arglist.append(updater_job_args.get("operation.type"))
        else:
            arglist.append('insert')
        arglist.append(
            f'{dag_config.get(consts.BIGQUERY).get(constants.DATASET)}.{segment_name}_{constants.TEMP_TABLE_SUFFIX}')
        arglist = logging_utils.build_spark_logging_info(dag_id='{{dag.dag_id}}', default_args=self.default_args,
                                                         arg_list=arglist)
        update_columns = updater_job_args.get("pcb.oracle.update.columns")
        if update_columns:
            arglist.append(f"pcb.oracle.update.columns={update_columns}")
        unique_key = updater_job_args.get("pcb.bigquery.loader.source.unique.key")
        if unique_key:
            arglist.append(f"pcb.bigquery.loader.source.unique.key={unique_key}")
        oracle_temp_table = updater_job_args.get("pcb.oracle.dest.temp.table")
        if oracle_temp_table:
            arglist.append(f"pcb.oracle.dest.temp.table={oracle_temp_table}")

        logger.info(f"Final Spark job arguments: {arglist}")
        spark_oracle_updater_job = {
            consts.REFERENCE: {consts.PROJECT_ID: self.dataproc_config.get(consts.PROJECT_ID)},
            consts.PLACEMENT: {consts.CLUSTER_NAME: cluster_name},
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: spark_config[UPDATE_JAR_URI],
                consts.MAIN_CLASS: spark_config[UPDATER_MAIN_CLASS],
                consts.FILE_URIS: spark_config[consts.FILE_URIS],
                consts.ARGS: arglist
            }
        }
        return DataprocSubmitJobOperator(
            task_id=f"{segment_name}_spark_oracle_updater_job",
            job=spark_oracle_updater_job,
            region=self.dataproc_config.get(consts.LOCATION),
            project_id=self.dataproc_config.get(consts.PROJECT_ID),
            gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID),
        )

    def create_insert_into_ora_task(self, cluster_name: str, dag_config: dict, body_segments: Union[list, set]):
        task_groups = []
        sequence = dag_config.get('sequence', False)
        previous_task = None

        for segment_name in body_segments:
            with TaskGroup(group_id=f"{segment_name}") as segment_group:
                temp_table_task = PythonOperator(
                    task_id=f'create_temp_table_{segment_name}',
                    python_callable=self.create_temp_tables,
                    op_kwargs={
                        'dag_config': dag_config,
                        'file_create_dt': "{{ dag_run.conf['file_create_dt'] | default('')}}",
                        'segment_name': segment_name,
                        'file_name': "{{ dag_run.conf['file_name'] | default('')}}"
                    }
                )

                skip_oracle_for_segment = False

                export_config = dag_config.get('gcs_export', {})
                export_segments = export_config.get('export_segments', None)
                skip_oracle_segments = export_config.get('skip_oracle_segments', None)

                if dag_config.get('skip_oracle_load', False):
                    skip_oracle_for_segment = True

                elif skip_oracle_segments is not None and segment_name in skip_oracle_segments:
                    skip_oracle_for_segment = True

                elif (
                    dag_config.get('skip_oracle_load_for_exports', False)
                    and export_segments is not None
                    and segment_name in export_segments
                ):
                    skip_oracle_for_segment = True

                export_gcs_task = None
                move_to_landing_task = None
                if dag_config.get('enable_gcs_export', False):
                    should_export = (
                        export_segments is None
                        or segment_name in export_segments
                    )

                    if should_export:
                        export_task_id = f'export_{segment_name}_to_gcs'
                        export_gcs_task = PythonOperator(
                            task_id=export_task_id,
                            python_callable=self.export_bq_table_to_gcs,
                            op_kwargs={
                                'dag_config': dag_config,
                                'segment_name': segment_name
                            }
                        )

                        compose_task = None
                        merge_config = export_config.get('merge_sharded_files', {})
                        if merge_config.get('enabled', False):
                            compose_task = PythonOperator(
                                task_id=f'compose_{segment_name}_sharded_files',
                                python_callable=self.compose_sharded_files_into_one,
                                op_kwargs={
                                    'dag_config': dag_config,
                                    'segment_name': segment_name
                                }
                            )

                        landing_config = export_config.get('move_to_landing', {})
                        if landing_config.get('enabled', False):
                            destination_bucket = landing_config.get('destination_bucket')
                            destination_folder = landing_config.get('destination_folder', 'bq_exports')
                            env_suffix = '' if self.deploy_env == 'prod' else f'-{self.deploy_env}'
                            destination_bucket = destination_bucket.replace('{env}', self.deploy_env).replace('{env_suffix}', env_suffix)
                            logger.info(f"file will be moved to : {destination_bucket} and folder : {destination_folder}")

                            xcom_source_task_id = f'{segment_name}.compose_{segment_name}_sharded_files' if compose_task else f'{segment_name}.{export_task_id}'

                            if compose_task:
                                segment_file_configs = export_config.get('segment_file_names', {})
                                segment_file_config = segment_file_configs.get(segment_name, {})
                                output_filename = segment_file_config.get('output_filename', segment_name)
                                filename_ext = segment_file_config.get('file_extension')
                                if filename_ext:
                                    file_extension = read_env_filepattern(filename_ext, self.deploy_env)
                                else:
                                    file_extension = export_config.get('file_format', 'CSV').lower()
                                destination_filename = f"{output_filename}_{JOB_DATE_TIME}.{file_extension}"
                            else:
                                destination_filename = f"{{{{ ti.xcom_pull(task_ids='{xcom_source_task_id}', key='{segment_name}_output_filename') }}}}"

                            move_to_landing_task = GCSToGCSOperator(
                                task_id=f'move_{segment_name}_to_landing',
                                gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID),
                                source_bucket=f"{{{{ ti.xcom_pull(task_ids='{xcom_source_task_id}', key='{segment_name}_gcs_bucket') }}}}",
                                source_object=f"{{{{ ti.xcom_pull(task_ids='{xcom_source_task_id}', key='{segment_name}_gcs_path') }}}}",
                                destination_bucket=destination_bucket,
                                destination_object=f"{destination_folder}/{destination_filename}"
                            )

                if skip_oracle_for_segment:
                    if export_gcs_task:
                        last_task = export_gcs_task
                        temp_table_task >> export_gcs_task

                        if compose_task:
                            export_gcs_task >> compose_task
                            last_task = compose_task

                        if move_to_landing_task:
                            last_task >> move_to_landing_task
                else:
                    cluster_task = self.build_cluster_creating_task(
                        job_size=dag_config.get('job_size'),
                        cluster_name=cluster_name
                    )

                    spark_task = self.oracle_table_updates(
                        cluster_name=cluster_name,
                        dag_config=dag_config,
                        segment_name=segment_name
                    )

                    if export_gcs_task:
                        temp_table_task >> [cluster_task, export_gcs_task]
                        cluster_task >> spark_task

                        last_task = export_gcs_task
                        if compose_task:
                            export_gcs_task >> compose_task
                            last_task = compose_task

                        if move_to_landing_task:
                            last_task >> move_to_landing_task
                    else:
                        temp_table_task >> cluster_task >> spark_task

            if sequence and previous_task:
                previous_task >> segment_group
            previous_task = segment_group

            task_groups.append(segment_group)

        return task_groups

    def postprocessing_job(self, dag_config: dict):
        pass

    def create_dag(self, dag_id: str, dag_config: dict) -> DAG:
        """
        Main DAG which is triggered to extract output file
        """
        cluster_name = dag_config.get('cluster_name')
        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            start_date=datetime(2023, 1, 1, tzinfo=self.local_tz),
            dagrun_timeout=timedelta(hours=5),
            max_active_runs=1,
            catchup=False,
            schedule=None,
            is_paused_upon_creation=True
        )
        with (dag):
            if dag_config.get(consts.READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(constants.output_file_creation, self.deploy_env)
                pause_unpause_dag(dag, is_paused)

            start = EmptyOperator(task_id='Start')
            segment_task_groups = self.create_insert_into_ora_task(
                cluster_name, dag_config, self.get_oracle_table_segment_list(dag_config))
            end = EmptyOperator(task_id='End')

            for group in segment_task_groups:
                start >> group >> end
        return add_tags(dag)

    def create_dags(self) -> dict:
        if self.job_config:
            dags = {}

            for job_id, dag_config in self.job_config.items():
                self.default_args.update(dag_config.get(consts.DEFAULT_ARGS, constants.INITIAL_DEFAULT_ARGS))
                dags[job_id] = self.create_dag(job_id, dag_config)
                dags[job_id].tags = dag_config.get(consts.TAGS)

            return dags
        else:
            return {}
