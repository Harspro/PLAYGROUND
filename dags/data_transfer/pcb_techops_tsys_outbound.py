import logging
import util.constants as consts
from typing import Final
from copy import deepcopy
from dataclasses import asdict
from datetime import timedelta, datetime
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.exceptions import AirflowFailException
from util.auditing_utils.audit_util import AuditUtil
from util.auditing_utils.model.audit_record import AuditRecord
from data_transfer.util.constants import DATA_TRANSFER_AUDIT_TABLE_NAME, DataTransferExecutionType, TRANSFERRED
from data_transfer.util.utils import create_data_transfer_audit_table_if_not_exists
from util.gcs_utils import gcs_file_exists
from dag_factory.abc import BaseDagBuilder
from dag_factory import DAGFactory
from google.cloud.storage import Client
from google.cloud.storage.blob import Blob


logger = logging.getLogger(__name__)
DAG_DEFAULT_ARGS: Final = {
    "owner": "team-centaurs",
    'capability': 'Terminus Data Platform',
    'severity': 'P2',
    'sub_capability': 'Operations',
    'business_impact': 'Operations would not be able to transfer file to the Tsys vendor which cause Customer will not be able to receive the refunds',
    'customer_impact': 'N/A',
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True
}


class PCBTechopsTsysOutboundFileTransfer(BaseDagBuilder):
    def prepare_config(self, **context: dict) -> None:
        create_data_transfer_audit_table_if_not_exists()
        conf = context['dag_run'].conf
        source_project_id = self.environment_config.gcp_config.get(
            consts.PROCESSING_ZONE_PROJECT_ID)
        source_file_path = conf.get('source_file_path')
        source_file_name = source_file_path.rsplit('/', 1)[-1]
        source_dag_id = context.get('dag').dag_id

        if source_file_path.startswith('gs://'):
            blob = Blob.from_string(source_file_path)
            source_bucket_name = blob.bucket.name
            source_file_path = blob.name
        else:
            raise AirflowFailException("source_file_path must start with 'gs://'")
        logger.info(f"""Source file info:
                        source_project_id: {source_project_id}
                        source_bucket_name: {source_bucket_name}
                        source_file_path: {source_file_path}
                        source_file_name: {source_file_name}
                """)

        destination_project_id = self.environment_config.gcp_config.get(
            consts.LANDING_ZONE_PROJECT_ID)
        logger.info("destination_project_id: %s", destination_project_id)

        # Get destination bucket from dag_config
        if not self.dag_config or source_dag_id not in self.dag_config:
            raise AirflowFailException(f"Configuration not found for DAG ID: {source_dag_id}")

        destination_bucket = self.dag_config[source_dag_id]['destination_bucket_name']
        destination_bucket = f'{destination_bucket}{self.environment_config.storage_suffix}'
        logger.info("destination_bucket: %s", destination_bucket)
        destination_file_path = source_file_path

        audit_record_values = f"""
                        '{source_project_id}',
                        '',
                        '',
                        'file',
                        'gs://{source_bucket_name}/{source_file_path}',
                        '{source_file_name}',
                        '{destination_project_id}',
                        'gs://{destination_bucket}/{destination_file_path}',
                        '{context.get('dag').dag_id}',
                        '{context.get('run_id')}',
                        '{DataTransferExecutionType.VENDOR_DATA_OUTBOUND.value}',
                        'manual',
                        '{source_project_id}',
                        CURRENT_TIMESTAMP()
          """

        audit_record = AuditRecord(consts.DOMAIN_TECHNICAL_DATASET_ID, DATA_TRANSFER_AUDIT_TABLE_NAME, audit_record_values)
        self.audit_util.record_request_received(audit_record)
        context['ti'].xcom_push(key='audit_record', value=asdict(audit_record))
        context['ti'].xcom_push(key='source_bucket_name', value=source_bucket_name)
        context['ti'].xcom_push(key='source_file_path', value=source_file_path)
        context['ti'].xcom_push(key='source_file_name', value=source_file_name)
        context['ti'].xcom_push(key='destination_bucket', value=destination_bucket)
        context['ti'].xcom_push(key='destination_file_path', value=destination_file_path)

    def validate_source_file(self, **context: dict) -> None:
        """
        Validate that the source file doesn't already have a .transferred version
        """
        source_bucket_name = context['ti'].xcom_pull(task_ids='prepare_config', key='source_bucket_name')
        source_file_path = context['ti'].xcom_pull(task_ids='prepare_config', key='source_file_path')
        source_file_name = context['ti'].xcom_pull(task_ids='prepare_config', key='source_file_name')

        # Validation: Check if file already has .transferred suffix
        if source_file_path.endswith(TRANSFERRED) or source_file_name.endswith(TRANSFERRED):
            raise AirflowFailException(f"Cannot transfer file '{source_file_name}' as it already has {TRANSFERRED} suffix. "
                                       f"This file has already been transferred to TSYS.")

        # Check if a .transferred version already exists on bucket
        transferred_file_path = f"{source_file_path}{TRANSFERRED}"

        logger.info(f"Checking if transferred file already exists: gs://{source_bucket_name}/{transferred_file_path}")

        if gcs_file_exists(source_bucket_name, transferred_file_path):
            raise AirflowFailException(f"A {TRANSFERRED} version of this file already exists: gs://{source_bucket_name}/{transferred_file_path}. "
                                       f"This indicates the file has already been transferred to TSYS. "
                                       f"Original file path: gs://{source_bucket_name}/{source_file_path}")

        logger.info(f"Validation passed: No {TRANSFERRED} version found for {source_file_path}")

        logger.info(f"Checking if {source_file_name} file found : gs://{source_bucket_name}/{source_file_path}")
        if not gcs_file_exists(source_bucket_name, source_file_path):
            raise AirflowFailException(f"File not found at {source_bucket_name}/{source_file_path}")

    def build(self, dag_id: str, config: dict) -> DAG:
        # Initialize audit_util and default_args
        self.audit_util = AuditUtil('prepare_config', 'audit_record')
        self.default_args = deepcopy(DAG_DEFAULT_ARGS)
        self.dag_config = {dag_id: config}

        with DAG(dag_id=dag_id,
                 schedule=None,
                 render_template_as_native_obj=True,
                 default_args=self.default_args,
                 start_date=datetime(2025, 1, 1, tzinfo=self.environment_config.local_tz),
                 catchup=False,
                 max_active_runs=1,
                 is_paused_upon_creation=True,
                 params={
                     'source_file_path': ''
                 }) as dag:
            start = EmptyOperator(task_id='start')
            end = EmptyOperator(task_id='end')

            prepare_config_task = PythonOperator(
                task_id="prepare_config",
                python_callable=self.prepare_config,
                on_failure_callback=self.audit_util.record_request_failure
            )

            validate_source_file_task = PythonOperator(
                task_id="validate_source_file",
                python_callable=self.validate_source_file,
                on_failure_callback=self.audit_util.record_request_failure
            )

            move_data_from_staging_to_landing = GCSToGCSOperator(
                task_id="move_data_from_staging_to_landing",
                source_bucket="{{ ti.xcom_pull(task_ids='prepare_config', key='source_bucket_name') }}",
                source_object="{{ ti.xcom_pull(task_ids='prepare_config', key='source_file_path') }}",
                destination_bucket="{{ ti.xcom_pull(task_ids='prepare_config', key='destination_bucket') }}",
                destination_object="{{ ti.xcom_pull(task_ids='prepare_config', key='destination_file_path') }}",
                on_failure_callback=self.audit_util.record_request_failure,
                on_success_callback=self.audit_util.record_request_success
            )

            rename_source_file_task = GCSToGCSOperator(
                task_id="rename_source_file_with_transferred_suffix",
                source_bucket="{{ ti.xcom_pull(task_ids='prepare_config', key='source_bucket_name') }}",
                source_object="{{ ti.xcom_pull(task_ids='prepare_config', key='source_file_path') }}",
                destination_bucket="{{ ti.xcom_pull(task_ids='prepare_config', key='source_bucket_name') }}",
                destination_object=f"{{{{ ti.xcom_pull(task_ids='prepare_config', key='source_file_path') }}}}{TRANSFERRED}",
                move_object=True,
                on_failure_callback=self.audit_util.record_request_failure
            )

            start >> prepare_config_task >> validate_source_file_task >> move_data_from_staging_to_landing >> rename_source_file_task >> end

        return dag


# Create DAG using the factory pattern
globals().update(DAGFactory().create_dynamic_dags(PCBTechopsTsysOutboundFileTransfer, 'pcb_techops_tsys_outbound_config.yaml', f'{settings.DAGS_FOLDER}/data_transfer/config'))
