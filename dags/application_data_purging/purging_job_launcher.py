import logging
import os
from datetime import timedelta
from typing import Final

import pendulum
from airflow import DAG, settings
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator
)

import util.constants as consts
from dag_factory.terminus_dag_factory import add_tags
from util.gcs_oci_transfer_utils import gcs_to_oci_file_transfer_task, summarize_transfer_results_task
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env,
    get_ephemeral_cluster_config
)

logger = logging.getLogger(__name__)

PURGING_PROCESSOR_PREFIX: Final = 'pcb.purging.processor'
DAG_ID: Final = 'application_data_purging'
GS_TAG: Final[str] = "team-growth-and-sales"
CUTOFF_DATE: Final = 'cutoff_date'
BATCH_STATUS: Final = 'batch_status'
ADM_GCP_SOUCRE: Final = 'adm_gcp_source'  # default value defined in config as "N" and at run time need to define as "Y" for GCP source
REPARTITION_COUNT: Final = 'repartition_count'  # default value defined in config as 100 and at run time pass appropriate value.

"""Example of run time config params to pass
{
    "adm_gcp_source": "Y",
    "cutoff_date": "20240525",
    "repartition_count": "50",
    "oci_destination_url" : "https://"
}
"""


class PurgingJobLauncher:
    def __init__(self, config_filename: str = None, config_dir: str = None):

        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)
        self.ephemeral_cluster_name = 'application-data-purging-ephemeral'

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/application_data_purging/config'

        if config_filename is None:
            config_filename = 'application_data_purging_config.yaml'

        self.job_config = read_yamlfile_env(f'{config_dir}/{config_filename}', self.deploy_env)

        self.default_args = {
            'owner': 'team-growth-and-sales-alerts',
            'capability': 'CustomerAcquisition',
            'severity': 'P3',
            'sub_capability': 'Applications',
            'business_impact': 'Compliance Issue if the data is not purged',
            'customer_impact': 'None',
            'depends_on_past': False,
            "email": [],
            "retries": 0,
            "email_on_failure": False,
            "email_on_retry": False
        }

        self.local_tz = pendulum.timezone(consts.TORONTO_TIMEZONE_ID)

    def generate_pause_file_names(self):
        """Generate pause file names based on the report config file tables."""
        report_config_path = f'{settings.DAGS_FOLDER}/application_data_purging/config/application_data_purging_report_config.yaml'
        report_config = read_yamlfile_env(report_config_path, self.deploy_env)
        pause_files = []
        env_prefix = "IT" if self.deploy_env.lower() in ['uat', 'test', 'dev'] else "PR"

        tables_config = report_config.get('sql.script.generator', {}).get('tables', {})
        for tables in tables_config.values():
            for table in tables:
                table_name = table.get('name')
                schemas = table.get('schemas', [])

                if isinstance(schemas, str):
                    schemas = [s.strip() for s in schemas.split(',')]
                elif schemas is None:
                    schemas = []

                logger.info(f"Table {table_name} has {len(schemas)} schemas: {schemas}")

                for schema in schemas:
                    # Parse schema format: PR053.ADM -> prefix=053, schema=ADM
                    schema_parts = schema.split('.')
                    if len(schema_parts) == 2:
                        prefix_part = schema_parts[0]  # PR053
                        schema_name = schema_parts[1]  # ADM

                        # Extract numeric prefix (053, 303, 563)
                        numeric_prefix = prefix_part.replace('PR', '')

                        # Generate pause file name with environment-specific prefix
                        pause_file_name = f"{env_prefix}{numeric_prefix}_{schema_name}_{table_name}_pause.txt"
                        pause_files.append(pause_file_name)

        pause_files = sorted(list(set(pause_files)))
        logger.info(f"Generated {len(pause_files)} pause file names with prefix {env_prefix} for environment {self.deploy_env}")
        return pause_files

    def get_delete_procedures_files(self, folder_name):
        """Get list of files from a specified folder in GCS."""
        file_transfer_config = self.job_config.get('file_transfer', {})
        gcs_bucket = file_transfer_config.get('gcs_source_bucket').format(env=self.deploy_env)
        gcs_base_path = file_transfer_config.get('gcs_source_path', 'purging/')
        folder_path = f"{gcs_base_path.rstrip('/')}/{folder_name}/"

        gcs_hook = GCSHook(gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID))

        try:
            folder_files = []
            blobs = gcs_hook.list(bucket_name=gcs_bucket, prefix=folder_path)

            for blob_name in blobs:
                # Extract the relative path from the folder
                if blob_name.startswith(folder_path) and blob_name != folder_path:
                    relative_path = blob_name[len(gcs_base_path):]
                    # Skip folders (items ending with /)
                    if relative_path and not relative_path.endswith('/'):
                        folder_files.append({
                            'file': relative_path,
                            'gcs_path': blob_name,
                            'oci_path': relative_path
                        })

            logger.info(f"Found {len(folder_files)} files in {folder_name} folder")
            return folder_files

        except Exception as e:
            logger.error(f"Error listing files in {folder_name} folder: {str(e)}")
            raise AirflowFailException(f"Error listing files in {folder_name} folder") from e

    def create_pause_files_in_gcs(self, **context):
        """Create empty pause files in GCS bucket."""
        file_transfer_config = self.job_config.get('file_transfer', {})
        gcs_bucket = file_transfer_config.get('gcs_source_bucket').format(env=self.deploy_env)
        gcs_path = file_transfer_config.get('gcs_source_path', '')

        gcs_hook = GCSHook(gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID))
        pause_files = self.generate_pause_file_names()

        logger.info(f"Creating {len(pause_files)} pause files in GCS")

        for pause_file in pause_files:
            try:
                gcs_object_name = f"{gcs_path.rstrip('/')}/{pause_file}"
                empty_content = b"empty file"

                gcs_hook.upload(
                    bucket_name=gcs_bucket,
                    object_name=gcs_object_name,
                    data=empty_content
                )

                logger.info(f"Created pause file: {pause_file}")

            except Exception as e:
                logger.error(f"Error creating pause file {pause_file}: {str(e)}")
                raise AirflowFailException(f"Error creating pause file {pause_file}") from e

        logger.info("All pause files created successfully in GCS")

    def build_transfer_items(self, file_transfer_config: dict, gcs_path: str, pause_files: list) -> list:
        """Build a comprehensive list of files to transfer from GCS to OCI."""
        transfer_items = []

        # 1. Add all files from configured folders
        folders_to_transfer = file_transfer_config.get('folders_to_transfer', [])
        total_folder_files = 0
        for folder_name in folders_to_transfer:
            delete_procedure_files = self.get_delete_procedures_files(folder_name)
            transfer_items.extend([{
                'file': item['file'],
                'gcs_path': item['gcs_path'],
                'oci_path': item['oci_path'],
                'source': f'{folder_name} folder'
            } for item in delete_procedure_files])
            total_folder_files += len(delete_procedure_files)

        # 2. Add configured individual files
        individual_files = file_transfer_config.get('files_to_transfer', [])
        for filename in individual_files:
            transfer_items.append({
                'file': filename,
                'gcs_path': f"{gcs_path.rstrip('/')}/{filename}",
                'oci_path': filename,
                'source': 'configured file'
            })

        # 3. Add pause files
        for filename in pause_files:
            transfer_items.append({
                'file': filename,
                'gcs_path': f"{gcs_path.rstrip('/')}/{filename}",
                'oci_path': filename,
                'source': 'pause file'
            })

        logger.info(f"Built transfer list: {len(individual_files)} configured files, "
                    f"{len(pause_files)} pause files, "
                    f"{total_folder_files} files from {len(folders_to_transfer)} folders")

        return transfer_items

    def prepare_transfer_data(self, **context):
        """Prepare list of transfer items for Dynamic Task Mapping."""
        file_transfer_config = self.job_config.get('file_transfer', {})
        gcs_path = file_transfer_config.get('gcs_source_path', '')

        dag_run_conf = context.get('dag_run', {}).conf or {}
        oci_base_url = dag_run_conf.get('oci_destination_url')

        if not oci_base_url:
            raise AirflowFailException("OCI destination URL not provided in dag_run.conf")

        pause_files = self.generate_pause_file_names()
        transfer_items = self.build_transfer_items(
            file_transfer_config=file_transfer_config,
            gcs_path=gcs_path,
            pause_files=pause_files
        )

        logger.info(f"Prepared {len(transfer_items)} files for Dynamic Task Mapping transfer")

        return transfer_items

    def create_pause_files_task(self):
        """Create a task for creating pause files in GCS."""
        pause_files_task = PythonOperator(
            task_id='create_pause_files',
            python_callable=self.create_pause_files_in_gcs
        )

        return pause_files_task

    def create_gcs_to_oci_transfer_data_task(self):
        """Create task to prepare GCS to OCI transfer data for Dynamic Task Mapping."""
        gcs_to_oci_data_task = PythonOperator(
            task_id='prepare_gcs_to_oci_transfer_data',
            python_callable=self.prepare_transfer_data
        )
        return gcs_to_oci_data_task

    def create_task(self):
        spark_job_config = self.job_config[consts.SPARK_JOB]

        cutoff_date = "{{ dag_run.conf.get('cutoff_date')  or '" \
                      + str(self.job_config[consts.ARGS][CUTOFF_DATE]) + "' }}"

        adm_gcp_source = "{{ dag_run.conf.get('adm_gcp_source')  or '" \
                         + str(self.job_config[consts.ARGS][ADM_GCP_SOUCRE]) + "' }}"

        repartition_count = "{{ dag_run.conf.get('repartition_count')  or '" \
                            + str(self.job_config[consts.ARGS][REPARTITION_COUNT]) + "' }}"

        arg_list = [f'{PURGING_PROCESSOR_PREFIX}.cutoff.date={cutoff_date}',
                    f'{PURGING_PROCESSOR_PREFIX}.batch.status=SCRIPT',
                    f'{PURGING_PROCESSOR_PREFIX}.adm.gcp.source={adm_gcp_source}',
                    f'{PURGING_PROCESSOR_PREFIX}.repartition.count={repartition_count}']

        spark_job = {
            consts.REFERENCE: {consts.PROJECT_ID: self.dataproc_config.get(consts.PROJECT_ID)},
            consts.PLACEMENT: {consts.CLUSTER_NAME: self.ephemeral_cluster_name},
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: spark_job_config[consts.JAR_FILE_URIS],
                consts.MAIN_CLASS: spark_job_config[consts.MAIN_CLASS],
                consts.FILE_URIS: spark_job_config[consts.FILE_URIS],
                consts.PROPERTIES: spark_job_config[consts.PROPERTIES],
                consts.ARGS: arg_list
            }
        }

        purging_task = DataprocSubmitJobOperator(
            task_id='run_purging_job',
            job=spark_job,
            region=self.dataproc_config.get(consts.LOCATION),
            project_id=self.dataproc_config.get(consts.PROJECT_ID),
            gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID)
        )

        return purging_task

    def create_dag(self, dag_id: str) -> DAG:
        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            schedule=None,
            catchup=False,
            is_paused_upon_creation=True,
            start_date=pendulum.today(self.local_tz).add(days=-2),
            dagrun_timeout=timedelta(hours=5),
            tags=[GS_TAG]
        )

        with dag:
            start_point = EmptyOperator(task_id='start')
            end_point = EmptyOperator(task_id='end')

            cluster_creating_task = DataprocCreateClusterOperator(
                task_id=consts.CLUSTER_CREATING_TASK_ID,
                project_id=self.dataproc_config.get(consts.PROJECT_ID),
                cluster_config=get_ephemeral_cluster_config(self.deploy_env, self.gcp_config.get(consts.NETWORK_TAG),
                                                            10),
                region=self.dataproc_config.get(consts.LOCATION),
                cluster_name=self.ephemeral_cluster_name
            )

            puring_task = self.create_task()
            pause_files_task = self.create_pause_files_task()
            gcs_to_oci_data_task = self.create_gcs_to_oci_transfer_data_task()

            # Create parallel GCS to OCI transfer tasks using Dynamic Task Mapping with partial()
            file_transfer_config = self.job_config.get('file_transfer', {})
            gcs_to_oci_transfer_results = gcs_to_oci_file_transfer_task.partial(
                gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID),
                gcs_bucket=file_transfer_config.get('gcs_source_bucket').format(env=self.deploy_env),
                oci_base_url="{{ dag_run.conf.get('oci_destination_url') }}"
            ).override(
                max_active_tis_per_dag=10
            ).expand(
                item=gcs_to_oci_data_task.output
            )

            summarize_gcs_to_oci_transfer_task = summarize_transfer_results_task(gcs_to_oci_transfer_results)

            start_point >> cluster_creating_task >> puring_task >> pause_files_task >> gcs_to_oci_data_task >> gcs_to_oci_transfer_results >> summarize_gcs_to_oci_transfer_task >> end_point
        return add_tags(dag)

    def create(self) -> dict:
        logger.info(f"job config. type: {type(self.job_config)}, value: {self.job_config}")
        return {DAG_ID: self.create_dag(DAG_ID)}


globals().update(PurgingJobLauncher().create())
