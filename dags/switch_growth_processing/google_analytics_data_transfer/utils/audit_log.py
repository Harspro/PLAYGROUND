"""
Audit logging for BigQuery table copy operations (Google Analytics migration).

Provides create_audit_table and log_changes_to_audit_table
for tracking table copy operations between projects.

Uses GA4DataTransferAuditUtil, a subclass of AuditUtil, for operational
audit logging; create_audit_table is custom to this DAG.
"""
import logging

from airflow.exceptions import AirflowException
from google.cloud import bigquery
from google.cloud.bigquery.table import TableReference

from util.auditing_utils.audit_util import AuditUtil
from util.auditing_utils.model.audit_record import AuditRecord

logger = logging.getLogger(__name__)


def combine_status(copy_status: str, validation_status: str) -> str:
    """
    Combine copy_status and validation_status into a single status value.

    Status priority:
    - If copy failed -> 'FAILED'
    - If copy succeeded but validation failed -> 'VALIDATION_FAILED'
    - If both succeeded -> 'SUCCESS'

    :param copy_status: Copy operation status ('SUCCESS' or 'FAILED')
    :param validation_status: Validation status ('SUCCESS' or other)
    :return: Combined status string
    """
    if copy_status != "SUCCESS":
        return "FAILED"
    elif validation_status != "SUCCESS":
        return "VALIDATION_FAILED"
    else:
        return "SUCCESS"


class GA4DataTransferAuditUtil(AuditUtil):
    """
    Audit util for GA4 data transfer (Google Analytics table copy) operations.

    Extends AuditUtil for logging table copy results (audit_record + status)
    when the record and status are built at call site.
    Since this class uses _insert_into_audit_table directly (not xcom-based methods),
    audit_record_task_id and audit_record_key are optional.
    """

    def __init__(self, audit_record_task_id: str = "", audit_record_key: str = ""):
        """
        Initialize GA4DataTransferAuditUtil.

        :param audit_record_task_id: Optional task ID for xcom (not used by _insert_into_audit_table)
        :param audit_record_key: Optional key for xcom (not used by _insert_into_audit_table)
        """
        super().__init__(audit_record_task_id, audit_record_key)

    def build_audit_record_values(
        self,
        dag_run_id: str,
        dag_id: str,
        task_id: str,
        copy_result: dict,
    ) -> str:
        """
        Build audit_record_values string following AuditUtil pattern.

        Creates comma-separated values string matching our schema (without status field).
        Status will be added by AuditUtil._insert_into_audit_table.
        error_msg is included in the values string.

        :param dag_run_id: Airflow DAG run ID
        :param dag_id: Airflow DAG ID
        :param task_id: Task ID
        :param copy_result: Copy result dictionary (may contain 'error_msg' key)
        :return: Comma-separated values string for INSERT (without status field, but includes error_msg)
        """
        # Handle NULL values for integers
        source_row_count = copy_result.get('source_row_count')
        target_row_count = copy_result.get('target_row_count')
        source_row_count_val = str(source_row_count) if source_row_count is not None else 'NULL'
        target_row_count_val = str(target_row_count) if target_row_count is not None else 'NULL'

        # Handle error_msg (NULL if not present or None)
        error_msg = copy_result.get('error_msg')
        if error_msg:
            error_msg_escaped = error_msg.replace("'", "''")
            error_msg_val = f"'{error_msg_escaped}'"
        else:
            error_msg_val = "NULL"

        # Escape string values
        dag_run_id_escaped = dag_run_id.replace("'", "''")
        dag_id_escaped = dag_id.replace("'", "''")
        task_id_escaped = task_id.replace("'", "''")
        source_table_ref_escaped = copy_result.get('source_table', '').replace("'", "''")
        target_table_ref_escaped = copy_result.get('target_table', '').replace("'", "''")

        audit_record_values = (
            f"'{dag_run_id_escaped}', "
            f"'{dag_id_escaped}', "
            f"'{task_id_escaped}', "
            f"'{source_table_ref_escaped}', "
            f"'{target_table_ref_escaped}', "
            f"{source_row_count_val}, "
            f"{target_row_count_val}, "
            f"CURRENT_DATETIME('America/Toronto'), "
            f"{error_msg_val}"
        )

        return audit_record_values

    def log_changes_to_audit_table(
        self,
        audit_table_ref: str,
        dag_run_id: str,
        dag_id: str,
        copy_results: list,
    ) -> None:
        """
        Write multiple copy operation results to the audit table.

        Uses GA4DataTransferAuditUtil._insert_into_audit_table for each record.
        Combines copy_status and validation_status into a single status field.

        :param audit_table_ref: Full table reference for audit table
        :param dag_run_id: Airflow DAG run ID
        :param dag_id: Airflow DAG ID
        :param copy_results: List of tuples (task_id, copy_result_dict) to log
        """
        if not copy_results:
            logger.warning("No copy results provided for batch audit logging")
            return

        # Parse audit_table_ref to extract dataset_id and table_id (AuditUtil pattern)
        table_ref = TableReference.from_string(audit_table_ref)
        dataset_id = table_ref.dataset_id
        table_id = table_ref.table_id

        # Process each copy result and insert using GA4DataTransferAuditUtil._insert_into_audit_table
        for task_id, copy_result in copy_results:
            if not copy_result:
                logger.warning(f"Skipping empty copy_result for task_id: {task_id}")
                continue

            # Combine copy_status and validation_status into single status
            copy_status = copy_result.get('copy_status', 'FAILED')
            validation_status = copy_result.get('validation_status', 'ERROR')
            combined_status = combine_status(copy_status, validation_status)

            # Build audit_record_values string (includes error_msg, but not status - insert adds status)
            audit_record_values = self.build_audit_record_values(
                dag_run_id=dag_run_id,
                dag_id=dag_id,
                task_id=task_id,
                copy_result=copy_result,
            )

            # Create AuditRecord (AuditUtil pattern)
            audit_record = AuditRecord(
                dataset_id=dataset_id,
                table_id=table_id,
                audit_record_values=audit_record_values
            )

            try:
                self._insert_into_audit_table(audit_record, combined_status)
                logger.debug(f"Logged audit record for task_id: {task_id}, status: {combined_status}")
            except Exception as e:
                error_msg = f"Failed to insert audit record for task_id '{task_id}': {e}"
                logger.error(error_msg)
                raise AirflowException(error_msg) from e

        logger.info(f"Logged {len(copy_results)} copy operation(s) to audit table: {audit_table_ref}")

    def create_audit_table(self, audit_table_ref: str) -> None:
        """
        Create an audit table to track table copy operations.

        :param audit_table_ref: Full table reference for audit table (project.dataset.table)
        """
        client = bigquery.Client()
        audit_table = TableReference.from_string(audit_table_ref)

        # Define audit table schema
        # Note: Uses single 'status' field (instead of copy_status/validation_status) to match AuditUtil pattern
        # error_msg is placed before status so it can be included in audit_record_values
        schema = [
            bigquery.SchemaField("dag_run_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("dag_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("task_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("source_table_ref", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("target_table_ref", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("source_row_count", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("target_row_count", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("created_at", "DATETIME", mode="REQUIRED"),
            bigquery.SchemaField("error_msg", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        ]

        table = bigquery.Table(audit_table, schema=schema)

        # Set table description
        table.description = "Audit log for BigQuery table copy operations between projects"

        client.create_table(table, exists_ok=True)
        logger.info(f"Audit table created or already exists: {audit_table_ref}")
