import logging
import pendulum
import util.constants as consts

from dataclasses import asdict
from datetime import timedelta, datetime
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from data_transfer.util.constants import DATA_TRANSFER_AUDIT_TABLE_NAME, DataTransferExecutionType
from data_transfer.util.utils import create_data_transfer_audit_table_if_not_exists, \
    write_marker_file_to_business_bucket
from util.auditing_utils.audit_util import AuditUtil
from util.auditing_utils.model.audit_record import AuditRecord
from util.constants import DOMAIN_TECHNICAL_DATASET_ID
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env
)

from dag_factory.abc import BaseDagBuilder
from dag_factory import DAGFactory


logger = logging.getLogger(__name__)
gcp_config = read_variable_or_file(consts.GCP_CONFIG)
deploy_env = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
local_tz = pendulum.timezone('America/Toronto')
audit_util = AuditUtil('prepare_config', 'audit_record')
DAG_DEFAULT_ARGS = {
    "owner": "team-centaurs",
    'capability': 'Terminus Data Platform',
    'severity': 'P2',
    'sub_capability': 'NA',
    'business_impact': 'Business would not be able to transfer file to the vendors',
    'customer_impact': 'Impact on inbound/outbound file flow',
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True
}


def prepare_config(**context):
    create_data_transfer_audit_table_if_not_exists()
    conf = context['dag_run'].conf
    source_file_path = conf.get('source_file_path')
    source_file_name = source_file_path.rsplit('/', 1)[-1]
    logger.info("source_file_path: %s", source_file_path)

    lookup_key = conf.get('lookup_key')
    team_name = lookup_key[1]
    logger.info("team_name: %s", team_name)

    destination_project_id = f'pcb-{deploy_env}-landing'
    destination_bucket = conf.get('destination_bucket_name')
    destination_path = source_file_path
    logger.info("destination_bucket: %s", destination_bucket)
    logger.info("destination_filename: %s", destination_path)

    audit_record_values = f"""
                        '{conf.get('source_project_id')}',
                        '{conf.get('source_dag_id')}',
                        '{conf.get('source_dag_run_id')}',
                        '{conf.get('source_type')}',
                        'gs://{conf.get('source_bucket_name')}/{source_file_path}',
                        '{source_file_name}',
                        '{destination_project_id}',
                        'gs://{destination_bucket}/{destination_path}',
                        '{context.get('dag').dag_id}',
                        '{context.get('run_id')}',
                        '{DataTransferExecutionType.VENDOR_DATA_OUTBOUND.value}',
                        '{conf.get('trigger_type')}',
                        '{conf.get('source_project_id')}',
                        CURRENT_TIMESTAMP()
          """
    audit_record = AuditRecord(DOMAIN_TECHNICAL_DATASET_ID, DATA_TRANSFER_AUDIT_TABLE_NAME, audit_record_values)
    audit_util.record_request_received(audit_record)
    context['ti'].xcom_push(key='audit_record', value=asdict(audit_record))
    context['ti'].xcom_push(key='source_bucket_name', value=conf.get('source_bucket_name'))
    context['ti'].xcom_push(key='source_file_path', value=source_file_path)
    context['ti'].xcom_push(key='destination_bucket', value=destination_bucket)
    context['ti'].xcom_push(key='destination_path', value=destination_path)
    context['ti'].xcom_push(key='lookup_key', value=conf.get('lookup_key'))
    context['ti'].xcom_push(key='marker_file_bucket', value=conf.get('marker_file_bucket'))
    context['ti'].xcom_push(key='marker_file_prefix', value=conf.get('marker_file_prefix'))


class BusinessToLandingDataTransferJob(BaseDagBuilder):
    def build(self, dag_id: str, config: dict) -> DAG:
        with DAG(dag_id=dag_id,
                 schedule=None,
                 render_template_as_native_obj=True,
                 default_args=DAG_DEFAULT_ARGS,
                 start_date=datetime(2024, 10, 1, tzinfo=local_tz),
                 catchup=False,
                 max_active_runs=1,
                 is_paused_upon_creation=True
                 ) as dag:
            start = EmptyOperator(task_id='start')
            end = EmptyOperator(task_id='end')

            prepare_config_task = PythonOperator(
                task_id="prepare_config",
                python_callable=prepare_config,
                on_failure_callback=audit_util.record_request_failure
            )

            move_data_from_business_project = GCSToGCSOperator(
                task_id="move_data_from_business_project",
                source_bucket="{{ ti.xcom_pull(task_ids='prepare_config', key='source_bucket_name') }}",
                source_object="{{ ti.xcom_pull(task_ids='prepare_config', key='source_file_path') }}",
                destination_bucket="{{ ti.xcom_pull(task_ids='prepare_config', key='destination_bucket') }}",
                destination_object="{{ ti.xcom_pull(task_ids='prepare_config', key='destination_path') }}",
                on_failure_callback=audit_util.record_request_failure,
                on_success_callback=audit_util.record_request_success
            )

            write_marker_file_to_business_bucket_task = PythonOperator(
                task_id="write_marker_file_to_business_bucket",
                python_callable=write_marker_file_to_business_bucket,
                op_kwargs={
                    'marker_file_bucket': "{{ ti.xcom_pull(task_ids='prepare_config', key='marker_file_bucket') }}",
                    'marker_file_prefix': "{{ ti.xcom_pull(task_ids='prepare_config', key='marker_file_prefix') }}",
                    'task_id': 'move_data_from_business_project'
                },
                trigger_rule='none_skipped'
            )

            start >> prepare_config_task >> move_data_from_business_project >> write_marker_file_to_business_bucket_task >> end
        return dag


globals().update(DAGFactory().create_dag(BusinessToLandingDataTransferJob, 'business_to_landing_data_transfer_job'))
