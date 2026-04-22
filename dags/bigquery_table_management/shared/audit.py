import logging
from typing import Optional
from google.cloud import bigquery
from airflow.exceptions import AirflowException
from google.cloud.bigquery.table import RowIterator

from bigquery_table_management.shared.schema import get_current_schema

logger = logging.getLogger(__name__)

AUDIT_DATASET = "domain_technical"
AUDIT_TABLE = "BIGQUERY_TABLE_MANAGEMENT_AUDIT"


class AuditModule:
    """
    Handles audit logging for BigQuery schema operations.

    This module is responsible for maintaining an audit table that tracks:
        - Ensure audit table exists
        - Record executed scripts
        - Store schema snapshots
        - Enable idempotency checks

    The audit table is automatically created if it does not exist.
    """

    def __init__(self, project_id: str, timezone: str):
        """
        Initialize the AuditModule.

        Args:
            project_id (str): GCP project ID where the audit table will be created and maintained.
            timezone (str): Timezone string for audit logging (e.g., "America/Toronto").

        Side Effects:
            - Creates a BigQuery client.
            - Constructs fully qualified audit table ID.
            - Ensures audit table exists (creates if missing).
        """
        self.project_id = project_id
        self.timezone = timezone
        self.client = bigquery.Client(project=project_id)
        self.audit_table_id = f"{self.project_id}.{AUDIT_DATASET}.{AUDIT_TABLE}"
        self._ensure_table_exists()

    # ============================================================================
    # TABLE CREATION
    # ============================================================================
    def _ensure_table_exists(self):
        """
        Ensure the audit table exists in BigQuery.

        Creates the audit table with:
            - Required schema
            - Time partitioning on insert_timestamp
            - Clustering on table_id and version

        The operation is idempotent (exists_ok=True).

        Raises:
            AirflowException:
                If table creation fails.
        """

        schema = [
            bigquery.SchemaField("table_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("version", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("applied_script", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("insert_timestamp", "DATETIME", mode="REQUIRED"),
            bigquery.SchemaField("json_table_schema", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("metadata", "JSON", mode="NULLABLE"),
        ]

        table = bigquery.Table(self.audit_table_id, schema=schema)

        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="insert_timestamp",
        )

        table.clustering_fields = ["table_id", "version"]

        try:
            self.client.create_table(table, exists_ok=True)
            logger.info(f"Audit table ensured: {self.audit_table_id}")
        except Exception as e:
            raise AirflowException(f"Failed ensuring audit table exists: {e}") from e

    # ---------------------------------------------------------
    # Private Query Runner
    # ---------------------------------------------------------

    def _run_query(self, query: str, parameters: list) -> RowIterator:
        """
        Execute a parameterized BigQuery query.

        Args:
            query (str): SQL query string.
            parameters (list): List of BigQuery query parameters.

        Returns:
            google.cloud.bigquery.table.RowIterator:
                Query execution result iterator.

        Raises:
            AirflowException:
                If query execution fails.
        """
        job_config = bigquery.QueryJobConfig(query_parameters=parameters)
        try:
            return self.client.query_and_wait(query, job_config=job_config)
        except Exception as e:
            logger.error(f"BigQuery query failed: {e}")
            raise AirflowException(f"BigQuery query failed: {e}") from e

    # ---------------------------------------------------------
    # Public
    # ---------------------------------------------------------

    def is_version_applied(self, table_ref: str, version: int) -> bool:
        """
        Check whether the version has already been applied to a table.

        Used for idempotency enforcement.

        Args:
            table_ref (str): Fully qualified table reference.
            version (int): version of the last script applied.

        Returns:
            bool:
                True if a matching audit record exists.
                False otherwise.
        """

        query = f"""
       SELECT COUNT(1) AS cnt
       FROM `{self.audit_table_id}`
       WHERE table_id = @table_id
       AND version = @version
       """

        params = [
            bigquery.ScalarQueryParameter("table_id", "STRING", table_ref),
            bigquery.ScalarQueryParameter("version", "INT64", version),
        ]

        result = self._run_query(query, params)
        return list(result)[0].cnt > 0

    def get_latest_version(self, table_ref: str) -> Optional[int]:
        """
        Get latest applied version for a table.
        Returns None if no history exists.
        """

        query = f"""
        SELECT version
        FROM `{self.audit_table_id}`
        WHERE table_id = @table_id
        ORDER BY insert_timestamp DESC
        LIMIT 1
        """

        params = [bigquery.ScalarQueryParameter("table_id", "STRING", table_ref)]

        result = list(self._run_query(query, params))

        if not result:
            return None

        return result[0].version

    def log_execution(
        self,
        table_ref: str,
        version: int,
        script_path: str,
        bq_client: bigquery.Client,
    ):
        """
        Insert execution record into audit table.

        Captures current table schema and metadata automatically.

        Args:
            table_ref (str): Target table reference.
            version (int): version applied
            script_path (str): Executed script path.
            bq_client (bigquery.Client): BigQuery client for schema capture.

        Returns:
            None
        """
        # Capture schema and metadata
        schema_result = get_current_schema(bq_client, table_ref)
        schema_json = {"columns": schema_result["columns"]}
        metadata = schema_result.get("metadata")

        insert_query = f"""
       INSERT INTO `{self.audit_table_id}` (
           table_id,
           version,
           applied_script,
           insert_timestamp,
           json_table_schema,
           metadata
       )
       VALUES (
           @table_id,
           @version,
           @applied_script,
           CURRENT_DATETIME("{self.timezone}"),
           @schema_json,
           @metadata
       )
       """

        params = [
            bigquery.ScalarQueryParameter("table_id", "STRING", table_ref),
            bigquery.ScalarQueryParameter("version", "INT64", version),
            bigquery.ScalarQueryParameter("applied_script", "STRING", script_path),
            bigquery.ScalarQueryParameter("schema_json", "JSON", schema_json),
            bigquery.ScalarQueryParameter("metadata", "JSON", metadata),
        ]

        self._run_query(insert_query, params)

        logger.info(f"Audit logged | Table: {table_ref} | Script: {script_path}")
