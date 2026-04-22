"""
Clustering Processor for BigQuery Table Management Framework.

Applies clustering updates as part of versioned schema changes.
Uses clustering_utils to update clustering metadata and re-cluster existing data.

Extends BaseProcessor which handles common workflow:
- Version extraction
- Version-based idempotency check
- Version increment validation
- Standard audit logging

This processor handles:
- Loading clustering config from YAML
- State-based idempotency check (current vs desired clustering)
- Updating clustering via REST API
- Re-clustering existing data
"""

import logging
from typing import Dict

from airflow.exceptions import AirflowException

from bigquery_table_management.processors.base_processor import BaseProcessor
from bigquery_table_management.processors.clustering.clustering_utils import (
    update_table_clustering,
    get_current_clustering,
)
from bigquery_table_management.shared.constants import CLUSTERING_FIELDS

logger = logging.getLogger(__name__)


class ClusteringProcessor(BaseProcessor):
    """
    Processor for applying clustering updates to BigQuery tables.

    Applies clustering updates as versioned operations:
    - Reads clustering_fields from YAML config file
    - Validates table reference
    - Checks idempotency (current clustering vs desired)
    - Updates clustering via REST API
    - Re-clusters existing data

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
        Execute clustering update operation.

        Processor-specific workflow:
        1. Extract clustering_fields from config
        2. Replace environment placeholders
        3. Check current clustering (state-based idempotency)
        4. Update clustering via REST API and re-cluster data

        Args:
            script_path (str): Relative path to .yaml file inside DAGs folder.
            table_ref (str): Fully qualified expected table reference.
            version (int): Version number extracted from filename.
            config (dict): Configuration dictionary with CLUSTERING_FIELDS key.

        Returns:
            bool:
                True if clustering updated.
                False if skipped due to state-based idempotency
                (audit logging handled internally).

        Raises:
            AirflowException:
                If validation or execution fails.
        """
        # --------------------------------------------------------
        # 1. Extract Clustering Fields from Config
        # --------------------------------------------------------
        clustering_fields = config.get(CLUSTERING_FIELDS, [])

        if not clustering_fields and CLUSTERING_FIELDS not in config:
            raise AirflowException(
                f"No {CLUSTERING_FIELDS} found in {script_path}"
            )

        # Replace environment placeholders in field names (if any)
        clustering_fields = [
            field.replace("{env}", self.deploy_env)
            for field in clustering_fields
        ]

        # --------------------------------------------------------
        # 2. Check Current Clustering (State-based Idempotency)
        # --------------------------------------------------------
        current_clustering = get_current_clustering(table_ref)

        if current_clustering == clustering_fields:
            logger.info(
                f"Table {table_ref} already has desired clustering {clustering_fields}. "
                f"Skipping update but logging to audit."
            )
            # Log to audit for tracking (state-based skip case)
            self.audit.log_execution(
                table_ref=table_ref,
                version=version,
                script_path=script_path,
                bq_client=self.client,
            )
            return False

        # --------------------------------------------------------
        # 3. Update Clustering
        # --------------------------------------------------------
        logger.info(
            f"Updating clustering for {table_ref}: "
            f"{current_clustering} -> {clustering_fields}"
        )

        update_table_clustering(
            table_fqdn=table_ref,
            clustering_fields=clustering_fields,
            bq_client=self.client,
            location=self.location,
        )

        logger.info(f"Successfully updated clustering for {table_ref}")
        return True
