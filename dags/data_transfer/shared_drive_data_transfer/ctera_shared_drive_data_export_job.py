import logging
import pendulum
import util.constants as consts
import data_transfer.util.constants as dt_consts

from dataclasses import asdict
from dataclasses import dataclass

from datetime import timedelta, datetime
from airflow.exceptions import AirflowException
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from data_transfer.util.utils import create_data_transfer_audit_table_if_not_exists, \
    write_marker_file_to_business_bucket

from util.auditing_utils.audit_util import AuditUtil
from util.auditing_utils.model.audit_record import AuditRecord
from util.smb_utils import SMBUtil
from util.miscutils import resolve_smb_server_config, read_yamlfile_env
from dag_factory.terminus_dag_factory import add_tags

DAG_DEFAULT_ARGS = {
    "owner": "team-centaurs",
    'capability': 'Data Movement PDS',
    'severity': 'P3',
    'sub_capability': 'Data Enablement',
    'business_impact': 'Business shared data to BQ from Ctera shared drive',
    'customer_impact': 'N/A',
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True
}

logger = logging.getLogger(__name__)


@dataclass
class ConnectionInfo:
    destination: str
    environment: str


audit_util = AuditUtil('prepare_config', 'audit_record')


def prepare_config(**context):
    conf = context['dag_run'].conf
    context['ti'].xcom_push(key='marker_file_bucket', value=conf.get('marker_file_bucket'))
    context['ti'].xcom_push(key='marker_file_prefix', value=conf.get('marker_file_prefix'))
    create_data_transfer_audit_table_if_not_exists()
    source_path = conf.get('source_path')
    destination_file_name = source_path.rsplit('/', 1)[-1]
    lookup_key = conf.get('lookup_key')
    conn_info = ConnectionInfo(destination=lookup_key[0], environment=lookup_key[1])
    context['ti'].xcom_push(key='connection_info', value=asdict(conn_info))
    audit_record_values = f"""
        '{conf.get('source_project_id')}',
        '{conf.get('source_dag_id')}',
        '{conf.get('source_dag_run_id')}',
        '{conf.get('source_type')}',
        'gs://{conf.get('source_bucket')}/{source_path}',
        '{destination_file_name}',
        '{conf.get('source_project_id')}',
        '{conf.get('remote_filepath')}',
        '{context.get('dag').dag_id}',
        '{context.get('run_id')}',
        '{dt_consts.DataTransferExecutionType.SHARED_DRIVE_DATA_EXPORT.value}',
        '{conf.get('trigger_type')}',
        '{conf.get('source_project_id')}',
        CURRENT_TIMESTAMP()
    """
    audit_record = AuditRecord(consts.DOMAIN_TECHNICAL_DATASET_ID, dt_consts.DATA_TRANSFER_AUDIT_TABLE_NAME, audit_record_values)
    audit_util.record_request_received(audit_record)
    context['ti'].xcom_push(key='audit_record', value=asdict(audit_record))
    context['ti'].xcom_push(key='source_bucket', value=conf.get('source_bucket'))
    context['ti'].xcom_push(key='source_path', value=source_path)
    context['ti'].xcom_push(key='remote_filepath', value=conf.get('remote_filepath'))
    context['ti'].xcom_push(key='destination_file_name', value=destination_file_name)


def move_data_to_remote_server(remote_filepath: str, destination_file_name: str,
                               source_bucket: str, source_path: str, connection_info: ConnectionInfo):
    shared_drive_config = read_yamlfile_env(dt_consts.SHARED_DRIVE_CONFIG_FILE_PATH)
    destination = connection_info.get('destination')
    environment = connection_info.get('environment')

    if destination not in shared_drive_config:
        raise AirflowException(f"The lookup key {destination} is not found. Valid options "
                               f"are {list(shared_drive_config.keys())}")
    if environment not in shared_drive_config.get(destination, {}):
        raise AirflowException(f"The lookup key {environment} is not found. Valid options "
                               f"are {list(shared_drive_config.get(destination, {}).keys())}")

    destination_info = shared_drive_config.get(destination, {}).get(environment, {})
    server_ip, username, password = resolve_smb_server_config(destination_info)
    ctera_util = SMBUtil(server_ip, username, password)

    ctera_shared_folder = destination_info.get(dt_consts.CTERA_SHARED_FOLDER)
    ctera_destination_folder = f"{ctera_shared_folder}/{remote_filepath}"
    ctera_util.make_dir_recursively(ctera_destination_folder)
    logger.info(f"""
                source bucket:{source_bucket}
                source path:{source_path}
                ctera destination folder:{ctera_destination_folder}
                ctera file name:{destination_file_name}""")
    status = ctera_util.copy_gcs_to_ctera(ctera_destination_folder, destination_file_name, source_bucket, None, source_path)
    logger.info(f"status:{status}")
    if status == consts.CTERA_FAIL:
        raise AirflowException(f"Error in Copying file from GCS to CTERA with status {status}")


def check_file_transfer_status(**context):
    task_instance = context['dag_run'].get_task_instance('move_data_to_remote_server')
    logger.info(f"********* This task checks for the status of {task_instance.task_id} *********")
    logger.info(f"State of {task_instance.task_id} ::: {task_instance.state.upper()}. Marking the DAG {task_instance.dag_id} as {task_instance.state.upper()}")
    if task_instance.state.upper() != consts.CTERA_SUCCESS:
        logger.error(f"Task {task_instance.task_id} ::: {consts.CTERA_FAIL}. Marking the DAG {task_instance.dag_id} as {consts.CTERA_FAIL}")
        raise AirflowException(f"Task {task_instance.task_id} failed. Hence, Failing the DAG.")


with DAG(dag_id='ctera_shared_drive_data_export_job',
         schedule=None,
         render_template_as_native_obj=True,
         default_args=DAG_DEFAULT_ARGS,
         start_date=datetime(2025, 1, 1, tzinfo=pendulum.timezone('America/Toronto')),
         catchup=False,
         max_active_runs=1,
         is_paused_upon_creation=True,
         dagrun_timeout=timedelta(hours=5)
         ) as dag:
    add_tags(dag)
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule='none_failed')

    prepare_config_task = PythonOperator(
        task_id="prepare_config",
        python_callable=prepare_config,
        on_failure_callback=audit_util.record_request_failure
    )

    move_data_to_remote_server_task = PythonOperator(
        task_id="move_data_to_remote_server",
        python_callable=move_data_to_remote_server,
        op_kwargs={
            'remote_filepath': "{{ ti.xcom_pull(task_ids='prepare_config', key='remote_filepath') }}",
            'destination_file_name': "{{ ti.xcom_pull(task_ids='prepare_config', key='destination_file_name') }}",
            'source_bucket': "{{ ti.xcom_pull(task_ids='prepare_config', key='source_bucket') }}",
            'source_path': "{{ ti.xcom_pull(task_ids='prepare_config', key='source_path') }}",
            'connection_info': "{{ ti.xcom_pull(task_ids='prepare_config', key='connection_info') }}"
        },
        on_failure_callback=audit_util.record_request_failure,
        on_success_callback=audit_util.record_request_success
    )

    write_marker_file_to_business_bucket_task = PythonOperator(
        task_id="write_marker_file_to_business_bucket",
        python_callable=write_marker_file_to_business_bucket,
        op_kwargs={
            'marker_file_bucket': "{{ ti.xcom_pull(task_ids='prepare_config', key='marker_file_bucket') }}",
            'marker_file_prefix': "{{ ti.xcom_pull(task_ids='prepare_config', key='marker_file_prefix') }}",
            'task_id': 'move_data_to_remote_server'
        },
        trigger_rule='none_skipped'
    )

    check_file_transfer_status_task = PythonOperator(
        task_id='check_file_transfer_status',
        python_callable=check_file_transfer_status,
        trigger_rule='all_done'
    )

    start >> prepare_config_task >> move_data_to_remote_server_task >> \
        write_marker_file_to_business_bucket_task >> check_file_transfer_status_task >> end
