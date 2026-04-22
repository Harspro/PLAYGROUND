import logging
from dataclasses import asdict
from datetime import timedelta
from typing import Final
from copy import deepcopy

import pendulum
from airflow import DAG, settings, __version__
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

import util.constants as consts
from data_transfer.util.constants import DATA_TRANSFER_AUDIT_TABLE_NAME, DataTransferExecutionType
from data_transfer.util.utils import create_data_transfer_audit_table_if_not_exists
from util.auditing_utils.audit_util import AuditUtil
from util.auditing_utils.model.audit_record import AuditRecord
from util.constants import DOMAIN_TECHNICAL_DATASET_ID
from util.gcs_utils import (
    get_business_bucket,
    get_bucket_project_id
)
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env
)
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)
DAG_DEFAULT_ARGS: Final = {
    "owner": "team-centaurs",
    'capability': 'Terminus Data Platform',
    'severity': 'P3',
    'sub_capability': 'Data Enablement',
    'business_impact': 'N/A',
    'customer_impact': 'N/A',
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True
}


class LandingToBusinessInboundFileTransfer:

    def __init__(self, config_filename: str, vendor: str):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.env_storage_suffix = self.gcp_config[consts.DEPLOY_ENV_STORAGE_SUFFIX]
        self.local_tz = pendulum.timezone(consts.TORONTO_TIMEZONE_ID)
        self.config_dir = f'{settings.DAGS_FOLDER}/data_transfer/vendor_data_transfer/{vendor}'
        self.job_config = read_yamlfile_env(f'{self.config_dir}/{config_filename}', self.deploy_env)
        self.audit_util = AuditUtil('prepare_config', 'audit_record')
        self.default_args = deepcopy(DAG_DEFAULT_ARGS)

    def prepare_config(self, **context):
        create_data_transfer_audit_table_if_not_exists()
        conf = context['dag_run'].conf
        logger.info("conf: %s", conf)
        logger.info("self.job_config: %s", self.job_config)
        source_bucket = conf.get('bucket')
        source_object = conf.get('name')
        source_object_split = source_object.rsplit('/')
        source_file_name = source_object_split[1]
        source_type = 'file'
        source_project_id = get_bucket_project_id(source_bucket)
        source_dag_id = context.get('dag').dag_id
        source_dag_run_id = context.get('run_id')
        destination_bucket = self.job_config[source_dag_id]['destination_config']['destination_bucket_name']
        if self.env_storage_suffix:
            destination_bucket = get_business_bucket(destination_bucket, self.env_storage_suffix)
        logger.info("destination_bucket: %s", destination_bucket)
        destination_filename = f"{source_object}"
        destination_project_id = get_bucket_project_id(destination_bucket)
        archive_file_path = f'{consts.TRANSFERRED}/{source_object}.{consts.TRANSFERRED}'
        audit_record_values = f"""
                               '{source_project_id}',
                               '{source_dag_id}',
                               '{source_dag_run_id}',
                               '{source_type}',
                               'gs://{source_bucket}/{source_object}',
                               '{source_file_name}',
                               '{destination_project_id}',
                               'gs://{destination_bucket}/{destination_filename}',
                               '{source_dag_id}',
                               '{source_dag_run_id}',
                               '{DataTransferExecutionType.VENDOR_DATA_INBOUND.value}',
                               '{context.get('dag_run').run_type}',
                               '{destination_project_id}',
                               CURRENT_TIMESTAMP()
                           """
        audit_record = AuditRecord(DOMAIN_TECHNICAL_DATASET_ID, DATA_TRANSFER_AUDIT_TABLE_NAME, audit_record_values)
        self.audit_util.record_request_received(audit_record)
        context['ti'].xcom_push(key='audit_record', value=asdict(audit_record))
        context['ti'].xcom_push(key='source_bucket_name', value=source_bucket)
        context['ti'].xcom_push(key='source_object', value=source_object)
        context['ti'].xcom_push(key='source_file_name', value=source_file_name)
        context['ti'].xcom_push(key='destination_bucket', value=destination_bucket)
        context['ti'].xcom_push(key='destination_filename', value=destination_filename)
        context['ti'].xcom_push(key='archive_file_path', value=archive_file_path)

    def create_dag(self, dag_id: str, dag_config: dict) -> DAG:
        self.default_args.update(dag_config.get(consts.DEFAULT_ARGS, DAG_DEFAULT_ARGS))
        dag = DAG(dag_id=dag_id,
                  schedule=None,
                  render_template_as_native_obj=True,
                  start_date=pendulum.today(self.local_tz).add(days=-2),
                  is_paused_upon_creation=True,
                  max_active_runs=1,
                  catchup=False,
                  default_args=self.default_args)

        with dag:
            start = EmptyOperator(task_id='start')
            end = EmptyOperator(task_id='end')

            prepare_config_task = PythonOperator(
                task_id="prepare_config",
                python_callable=self.prepare_config,
                on_failure_callback=self.audit_util.record_request_failure
            )

            move_data_to_business_project = GCSToGCSOperator(
                task_id="move_data_to_business_project",
                source_bucket="{{ ti.xcom_pull(task_ids='prepare_config', key='source_bucket_name') }}",
                source_object="{{ ti.xcom_pull(task_ids='prepare_config', key='source_object') }}",
                destination_bucket="{{ ti.xcom_pull(task_ids='prepare_config', key='destination_bucket') }}",
                destination_object="{{ ti.xcom_pull(task_ids='prepare_config', key='destination_filename') }}",
                on_failure_callback=self.audit_util.record_request_failure,
                on_success_callback=self.audit_util.record_request_success
            )

            file_archival_task = GCSToGCSOperator(
                task_id="archive_transferred_file",
                source_bucket="{{ ti.xcom_pull(task_ids='prepare_config', key='source_bucket_name') }}",
                source_object="{{ ti.xcom_pull(task_ids='prepare_config', key='source_object') }}",
                destination_bucket="{{ ti.xcom_pull(task_ids='prepare_config', key='source_bucket_name') }}",
                destination_object="{{ ti.xcom_pull(task_ids='prepare_config', key='archive_file_path') }}",
                move_object=True
            )

            start >> prepare_config_task >> move_data_to_business_project >> file_archival_task >> end

            return add_tags(dag)

    def create(self) -> dict:
        dags = {}
        logger.info("dags: %s", dags)
        for job_id, config in self.job_config.items():
            dags[job_id] = self.create_dag(job_id, config)
        return dags
