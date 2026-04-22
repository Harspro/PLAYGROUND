"""
Base Processor for BigQuery Table Management Framework.

Provides common workflow and initialization for all processors.
Uses Template Method pattern to enforce consistent execution flow.

Common Workflow:
1. Extract version from filename
2. Check idempotency (version-based)
3. Validate runtime version increment
4. Execute processor-specific work (_do_apply)
5. Log to audit table (if executed)
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict

from google.cloud import bigquery

from bigquery_table_management.shared.audit import AuditModule
from bigquery_table_management.shared.versioning import (
    extract_version_from_filename,
    validate_runtime_version_increment,
)

logger = logging.getLogger(__name__)


class BaseProcessor(ABC):
    """
    Abstract base class for BigQuery table operation processors.

    Provides common initialization and workflow enforcement using
    Template Method pattern. Subclasses implement processor-specific
    logic in _do_apply().

    Responsibilities:
        - Store environment context (project, dataset, deploy environment)
        - Initialize BigQuery client and audit module
        - Enforce common workflow (version extraction, idempotency, validation)
        - Handle standard audit logging
    """

    def __init__(self, project_id: str, dataset_id: str, deploy_env: str, timezone: str, location: str):
        """
        Initialize base processor configuration.

        Args:
            project_id (str): GCP project ID where operations will be executed.
            dataset_id (str): Target BigQuery dataset ID.
            deploy_env (str): Deployment environment identifiers (e.g., dev, uat, prod)
            timezone (str): Timezone string for audit logging (e.g., "America/Toronto").
            location (str): BigQuery location/region (e.g., "northamerica-northeast1").

        Side Effects:
            - Initializes BigQuery client.
            - Initializes audit module instance.
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.deploy_env = deploy_env
        self.timezone = timezone
        self.location = location
        self.client = bigquery.Client(project=project_id)
        self.audit = AuditModule(project_id, timezone)

    def apply(
        self, script_path: str, table_ref: str, config: Dict
    ) -> bool:
        """
        Apply processor operation using template method pattern.

        Common workflow enforced:
        1. Extract version from filename
        2. Check idempotency (version-based)
        3. Validate runtime version increment
        4. Execute processor-specific work (_do_apply)
        5. Log to audit table (if executed)

        Args:
            script_path (str): Relative path to script file.
            table_ref (str): Fully qualified expected table reference.
            config (dict): Configuration dictionary. Contents depend on processor type:
                - ClusteringProcessor: {"clustering_fields": [...]}
                - SQLProcessor: {"sql": "CREATE TABLE..."}

        Returns:
            bool:
                True if operation executed.
                False if skipped due to idempotency.

        Raises:
            AirflowException:
                If validation fails or execution errors occur.
        """
        logger.info(f"Processing script: {script_path}")

        # --------------------------------------------------------
        # 1. Extract version from filename
        # --------------------------------------------------------
        version_number = extract_version_from_filename(script_path)

        # --------------------------------------------------------
        # 2. Version-based idempotency check
        # --------------------------------------------------------
        if self.audit.is_version_applied(table_ref, version_number):
            logger.info(
                f"Skipping already applied script: {script_path} (version {version_number})"
            )
            return False

        # --------------------------------------------------------
        # 3. Runtime version increment validation
        # --------------------------------------------------------
        last_version = self.audit.get_latest_version(table_ref)
        validate_runtime_version_increment(
            new_version=version_number, last_applied_version=last_version
        )

        # --------------------------------------------------------
        # 4. Execute processor-specific work
        # --------------------------------------------------------
        executed = self._do_apply(
            script_path=script_path,
            table_ref=table_ref,
            version=version_number,
            config=config,
        )

        # --------------------------------------------------------
        # 5. Standard audit logging (only if executed)
        # --------------------------------------------------------
        if executed:
            self.audit.log_execution(
                table_ref=table_ref,
                version=version_number,
                script_path=script_path,
                bq_client=self.client,
            )

            logger.info(f"Successfully executed: {script_path}")

        return executed

    @abstractmethod
    def _do_apply(
        self,
        script_path: str,
        table_ref: str,
        version: int,
        config: Dict,
    ) -> bool:
        """
        Execute processor-specific operation logic.

        This method must be implemented by subclasses to perform
        the actual operation (e.g., update clustering, apply partitioning).

        If the processor needs to skip due to state-based idempotency
        (e.g., clustering already matches desired), it should:
        - Log to audit itself (using self.audit.log_execution())
        - Return False

        Args:
            script_path (str): Relative path to script file.
            table_ref (str): Fully qualified expected table reference.
            version (int): Version number extracted from filename.
            config (dict): Configuration dictionary. Contents depend on processor type.

        Returns:
            bool:
                True if operation executed.
                False if skipped (processor handles own audit logging if needed).

        Raises:
            AirflowException:
                If execution fails.
        """
        pass
