"""
YAML Handler for BigQuery Table Management Framework.

Routes YAML configuration files to appropriate specialized processors
based on content detection (clustering_fields, partition_field, etc.).

This handler acts as a dispatcher/router for unified YAML files,
allowing the framework to support multiple operation types through
a single file extension (.yaml).

Supported Operations:
    - Clustering updates (clustering_fields) -> ClusteringProcessor
    - Partitioning updates (partition_field) - Future
    - Complex schema evolution (transformations) - Future

Execution Flow:
1. Read YAML file
2. Detect operation type from content
3. Route to appropriate specialized processor
4. Return processor execution result
"""

import logging
import os
from typing import Dict

from airflow.settings import DAGS_FOLDER
from airflow.exceptions import AirflowException

from bigquery_table_management.handlers.base_handler import BaseHandler
from bigquery_table_management.processors.clustering import ClusteringProcessor
from bigquery_table_management.shared.constants import (
    CLUSTERING_FIELDS,
    PARTITION_FIELD,
    TRANSFORMATIONS,
)
from util.miscutils import read_yamlfile_env

logger = logging.getLogger(__name__)


class YamlHandler(BaseHandler):
    """
    YAML configuration handler that routes to specialized processors.

    This handler reads YAML files and detects the operation type based on
    content (e.g., clustering_fields, partition_field, transformations).
    It then delegates execution to the appropriate specialized processor.

    Extends:
        BaseHandler
    """

    def __init__(self, project_id: str, dataset_id: str, deploy_env: str, timezone: str, location: str):
        """
        Initialize YamlHandler.

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
        Execute YAML configuration by routing to appropriate handler.

        Workflow:
        1. Read YAML file
        2. Detect operation type from content
        3. Route to specialized processor (ClusteringProcessor, etc.)
        4. Return execution result

        Args:
            script_path (str): Relative path to .yaml file inside DAGs folder.
            table_ref (str): Fully qualified expected table reference.

        Returns:
            bool:
                True if operation executed.
                False if skipped due to idempotency.

        Raises:
            AirflowException:
                If YAML cannot be read, parsed, or operation type is unsupported.
        """
        logger.info(f"Processing YAML config: {script_path}")

        # Read YAML file
        yaml_config = self._read_yaml_config(script_path)

        # Route to appropriate processor based on content
        if CLUSTERING_FIELDS in yaml_config:
            logger.info("Detected clustering operation, routing to ClusteringProcessor")
            processor = ClusteringProcessor(self.project_id, self.dataset_id, self.deploy_env, self.timezone, self.location)
            # Pass the already-parsed YAML config to avoid re-reading the file
            return processor.apply(script_path, table_ref, config=yaml_config)

        elif PARTITION_FIELD in yaml_config:
            raise AirflowException(
                f"Partitioning handler not implemented yet. "
                f"Found {PARTITION_FIELD} in {script_path}"
            )

        elif TRANSFORMATIONS in yaml_config:
            raise AirflowException(
                f"Schema evolution handler not implemented yet. "
                f"Found {TRANSFORMATIONS} in {script_path}"
            )

        else:
            raise AirflowException(
                f"Unable to detect operation type in {script_path}. "
                f"Expected one of: {CLUSTERING_FIELDS}, {PARTITION_FIELD}, {TRANSFORMATIONS}"
            )

    def _read_yaml_config(self, path: str) -> Dict:
        """
        Read YAML configuration file using shared utility.

        Uses read_yamlfile_env from util.miscutils for consistency with
        the rest of the codebase. Handles environment placeholder replacement
        and converts utility exceptions to AirflowException for consistency.

        Args:
            path (str): Relative path to YAML config file.

        Returns:
            dict: Parsed YAML configuration.

        Raises:
            AirflowException: If file does not exist or cannot be read.
        """
        config_file = os.path.join(DAGS_FOLDER, path)
        config = read_yamlfile_env(config_file, deploy_env=self.deploy_env)

        if config is None:
            raise AirflowException(f"YAML config file not found: {config_file}")

        if not isinstance(config, dict):
            raise AirflowException(f"YAML file is empty or invalid: {config_file}")

        return config
