import logging

from airflow.utils.state import TaskInstanceState
from google.cloud import bigquery

from data_transfer.util.constants import DATA_TRANSFER_AUDIT_TABLE_DDL
from util.constants import GCP_CONFIG, DEPLOYMENT_ENVIRONMENT_NAME
from util.gcs_utils import write_file
from util.miscutils import read_variable_or_file, read_file_env


def create_data_transfer_audit_table_if_not_exists():
    env = read_variable_or_file(GCP_CONFIG)[DEPLOYMENT_ENVIRONMENT_NAME]
    bigquery.Client().query(read_file_env(DATA_TRANSFER_AUDIT_TABLE_DDL, env)).result()


def write_marker_file_to_business_bucket(
        marker_file_bucket: str,
        marker_file_prefix: str,
        task_id: str,
        **context,
) -> None:
    if marker_file_prefix and marker_file_bucket:
        ti = context["dag_run"].get_task_instance(task_id=task_id)
        if ti.state == TaskInstanceState.SUCCESS:
            marker_file_prefix = marker_file_prefix + '_' + TaskInstanceState.SUCCESS
        else:
            marker_file_prefix = marker_file_prefix + '_' + TaskInstanceState.FAILED
        write_file(
            bucket_name=marker_file_bucket,
            blob_name=marker_file_prefix,
            content='Terminus marker file for business projects.'
        )
    else:
        logging.info("[WARNING] marker_file_bucket and marker_file_prefix have not been set up yet. Please configure "
                     "the operators in the business projects.")
