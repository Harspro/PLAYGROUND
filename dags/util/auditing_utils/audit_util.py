"""
This file defines the AuditUtil class.
"""
from google.cloud import bigquery

from util.auditing_utils.model.audit_record import AuditRecord
from util.constants import GCP_CONFIG, DEPLOYMENT_ENVIRONMENT_NAME
from util.miscutils import read_variable_or_file


class AuditUtil:
    # The task id to pull the audit record XCOM value from. For example, if you constructed the AuditRecord in a task
    # with task_id "prepare_config" and did an XCOM push there, then you would specify this argument as 'prepare_config'
    audit_record_task_id: str

    # The key to use when pulling the audit record via XCOM. This is the key you specify when doing the XCOM push. Make
    # sure this argument matches the key you use.
    audit_record_key: str

    def __init__(self,
                 audit_record_task_id: str,
                 audit_record_key: str):
        """
        This function initialises an AuditUtil object.

        :param audit_record_task_id: the task id to pull the audit record XCOM value from
        :param audit_record_key: the key to use when pulling the audit record via XCOM
        """
        self.audit_record_task_id = audit_record_task_id
        self.audit_record_key = audit_record_key
        self.deploy_env = read_variable_or_file(GCP_CONFIG)[DEPLOYMENT_ENVIRONMENT_NAME]

    def _insert_into_audit_table(self, audit_record: AuditRecord, status: str):
        """
        This private helper function inserts the audit record received via XCOM into the table.
        """
        insert_into_table_string = f"""
        INSERT INTO `pcb-{self.deploy_env}-landing.{audit_record.dataset_id}.{audit_record.table_id}`
        VALUES
        ({audit_record.audit_record_values}, '{status}')
        """
        bigquery.Client().query(insert_into_table_string).result()

    def record_request_received(self, audit_record: AuditRecord):
        self._insert_into_audit_table(audit_record, 'received')

    def record_request_success(self, context):
        audit_record = AuditRecord(**context['ti'].xcom_pull(task_ids=self.audit_record_task_id, key=self.audit_record_key))
        self._insert_into_audit_table(audit_record, 'completed')

    def record_request_failure(self, context):
        audit_record = AuditRecord(**context['ti'].xcom_pull(task_ids=self.audit_record_task_id, key=self.audit_record_key))
        self._insert_into_audit_table(audit_record, 'failed')
