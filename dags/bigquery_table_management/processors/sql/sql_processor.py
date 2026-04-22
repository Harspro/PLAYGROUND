"""
SQL Processor for BigQuery Table Management Framework.

Executes SQL DDL scripts safely with validation and dry-run checks.
Uses BaseProcessor for common workflow (versioning, idempotency, audit).

This processor handles:
- SQL validation (safe DDL rules)
- Table reference validation
- Dry-run validation
- SQL execution
"""

import logging
from typing import Dict

from airflow.exceptions import AirflowException
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

from bigquery_table_management.processors.base_processor import BaseProcessor
from bigquery_table_management.shared.validation import (
    validate_is_safe_ddl,
    validate_table_ref_match,
)

logger = logging.getLogger(__name__)


class SQLProcessor(BaseProcessor):
    """
    Processor for executing SQL DDL scripts on BigQuery tables.

    Executes validated SQL scripts with:
    - Strict DDL validation
    - Table reference enforcement
    - Dry-run validation
    - SQL execution

    Common workflow (version extraction, idempotency, validation, audit)
    is handled by BaseProcessor.
    """

    def _do_apply(
        self,
        script_path: str,
        table_ref: str,
        version: int,
        config: Dict,
    ) -> bool:
        """
        Execute SQL DDL script safely.

        Processor-specific workflow:
        1. Extract SQL from config
        2. Validate safe DDL rules
        3. Validate table reference
        4. Dry-run validation
        5. Execute SQL

        Args:
            script_path (str): Relative path to SQL file inside DAGs folder.
            table_ref (str): Fully qualified expected table reference.
            version (int): Version number extracted from filename.
            config (dict): Configuration dictionary with "sql" key containing SQL content.

        Returns:
            bool:
                True if SQL executed successfully.

        Raises:
            AirflowException:
                If validation or execution fails.
        """
        # --------------------------------------------------------
        # 1. Extract SQL from Config
        # --------------------------------------------------------
        sql = config.get("sql")
        if not sql:
            raise AirflowException(
                f"No SQL content found in config for {script_path}"
            )

        # --------------------------------------------------------
        # 2. Strict DDL Validation
        # --------------------------------------------------------
        validate_is_safe_ddl(sql)

        # --------------------------------------------------------
        # 3. Table Reference Validation
        # --------------------------------------------------------
        validate_table_ref_match(sql, table_ref)

        # --------------------------------------------------------
        # 4. Dry Run Validation
        # --------------------------------------------------------
        self._dry_run(sql)

        # --------------------------------------------------------
        # 5. Execute SQL
        # --------------------------------------------------------
        self._execute(sql)

        logger.info(f"Successfully executed SQL for {table_ref}")
        return True

    def _dry_run(self, sql: str):
        """
        Perform BigQuery dry-run validation.

        Executes query with dry_run=True to validate:
            - Syntax correctness
            - Schema correctness
            - Permission validation

        Args:
            sql (str): SQL statement to validate.

        Raises:
            AirflowException:
                If dry run validation fails.
        """
        logger.info("Performing dry run validation...")

        try:
            job_config = bigquery.QueryJobConfig(
                dry_run=True,
                use_query_cache=False,
            )
            self.client.query_and_wait(sql, job_config=job_config, location=self.location)
        except BadRequest as e:
            logger.error(f"Dry run failed: {str(e)}")
            raise AirflowException(f"Dry run validation failed: {str(e)}") from e
        except Exception as e:
            raise AirflowException(f"Unexpected error during dry run: {str(e)}") from e

        logger.info("Dry run successful.")

    def _execute(self, sql: str):
        """
        Execute SQL statement in BigQuery.

        Args:
            sql (str): SQL statement to execute.

        Raises:
            AirflowException:
                If execution fails.
        """
        logger.info("Executing SQL...")
        try:
            job_config = bigquery.QueryJobConfig()
            self.client.query_and_wait(sql, job_config=job_config, location=self.location)
            logger.info("Execution completed.")
        except Exception as e:
            raise AirflowException(f"SQL execution failed: {str(e)}") from e
