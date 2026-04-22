"""
SQL Handler for BigQuery Table Management Framework.

Routes SQL script files to SQLProcessor for execution.
Acts as a router/dispatcher similar to YamlHandler.

Execution Flow:
1. Read SQL file
2. Replace environment placeholders
3. Route to SQLProcessor
4. Return processor execution result
"""

import logging
import os

from airflow.settings import DAGS_FOLDER
from airflow.exceptions import AirflowException

from bigquery_table_management.handlers.base_handler import BaseHandler
from bigquery_table_management.processors.sql import SQLProcessor

logger = logging.getLogger(__name__)


class SQLHandler(BaseHandler):
    """
    SQL script handler that routes to SQLProcessor.

    This handler reads SQL files and delegates execution to SQLProcessor,
    which handles validation, dry-run, execution, and audit logging.

    Extends:
        BaseHandler
    """

    def __init__(self, project_id: str, dataset_id: str, deploy_env: str, timezone: str, location: str):
        """
        Initialize SQLHandler.

        Args:
            project_id (str): GCP project ID where operations will be executed.
            dataset_id (str): Target BigQuery dataset ID.
            deploy_env (str): Deployment environment (dev/uat/prod).
            timezone (str): Timezone string for audit logging (e.g., "America/Toronto").
            location (str): BigQuery location/region (e.g., "northamerica-northeast1").
        """
        super().__init__(project_id, dataset_id, deploy_env, timezone, location)

    def execute(self, script_path: str, table_ref: str) -> bool:
        """
        Execute SQL DDL script by routing to SQLProcessor.

        Workflow:
        1. Read SQL file
        2. Replace environment placeholders
        3. Route to SQLProcessor
        4. Return execution result

        Args:
            script_path (str): Relative path to SQL file inside DAGs folder.
            table_ref (str): Fully qualified expected table reference.

        Returns:
            bool:
                True if script executed.
                False if skipped due to idempotency.

        Raises:
            AirflowException:
                If SQL file cannot be read or execution fails.
        """
        logger.info(f"Processing SQL script: {script_path}")

        # Read SQL file
        sql = self._read_sql(script_path)

        # Replace environment placeholders
        sql = sql.replace("{env}", self.deploy_env)

        # Route to SQLProcessor
        processor = SQLProcessor(self.project_id, self.dataset_id, self.deploy_env, self.timezone, self.location)
        return processor.apply(script_path, table_ref, config={"sql": sql})

    def _read_sql(self, path: str) -> str:
        """
        Read SQL file from DAGs directory.

        Args:
            path (str):
                Relative path to SQL file.

        Returns:
            str:
                SQL file contents.

        Raises:
            AirflowException:
                If file does not exist or cannot be read.
        """
        sql_file = os.path.join(DAGS_FOLDER, path)
        try:
            with open(sql_file, "r", encoding="utf-8") as f:
                return f.read()

        except FileNotFoundError as e:
            raise AirflowException(f"SQL file not found: {sql_file}") from e

        except Exception as e:
            raise AirflowException(
                f"Failed to read SQL file: {sql_file}. Error: {str(e)}"
            ) from e
