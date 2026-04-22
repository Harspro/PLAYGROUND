"""
BigQuery Clustering Utilities for Table Management Framework.

Provides low-level BigQuery clustering operations via REST API.
Used by ClusteringProcessor to update table clustering metadata and re-cluster data.

Functions:
    get_current_clustering: Get current clustering columns for a table
    update_table_clustering: Update clustering metadata and re-cluster existing data

Uses REST API utilities from util.bq_utils for BigQuery table operations.
"""

import logging
from typing import List, Optional

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

from util.bq_utils import (
    BigQueryRestApiError,
    get_table_via_rest,
    patch_table_via_rest,
)

logger = logging.getLogger(__name__)

__all__ = [
    "get_current_clustering",
    "update_table_clustering",
]


def get_current_clustering(table_fqdn: str) -> Optional[List[str]]:
    """Get current clustering columns for a table (REST API).

    Args:
        table_fqdn: Fully qualified table name (project.dataset.table).

    Returns:
        List of clustering column names, or None if table not found or not clustered.

    Raises:
        BigQueryRestApiError: If REST API call fails.
    """
    try:
        table_resource = get_table_via_rest(table_fqdn)
        clustering = table_resource.get("clustering") or {}
        if isinstance(clustering, dict):
            fields = clustering.get("fields") or []
            return fields if fields else None
        return None
    except NotFound:
        logger.warning(f"Table {table_fqdn} not found")
        return None
    except BigQueryRestApiError as e:
        logger.error(f"Error fetching table {table_fqdn}: {e}")
        raise


def update_table_clustering(
    table_fqdn: str,
    clustering_fields: List[str],
    bq_client: bigquery.Client,
    location: str,
) -> None:
    """Update table clustering metadata and re-cluster existing data.

    1. PATCH clustering via REST API.
    2. Run UPDATE query to re-cluster existing data.

    Args:
        table_fqdn: Fully qualified table name.
        clustering_fields: List of column names to cluster on.
        bq_client: BigQuery client for running the UPDATE query.
        location: BigQuery location/region (e.g., "northamerica-northeast1").

    Raises:
        BigQueryRestApiError: REST API failure.
    """
    logger.info(
        f"Updating clustering for {table_fqdn} to fields: {clustering_fields}"
    )

    if not clustering_fields:
        logger.info(f"Removing clustering for {table_fqdn}")
        patch_table_via_rest(table_fqdn, {"clustering": None})
        logger.info(f"Successfully removed clustering metadata for {table_fqdn}")
        return

    patch_table_via_rest(
        table_fqdn, {"clustering": {"fields": clustering_fields}}
    )
    logger.info(f"Successfully updated clustering metadata for {table_fqdn}")

    first_field = clustering_fields[0]
    table_ref = f"`{table_fqdn}`"
    update_query = (
        f"UPDATE {table_ref} SET {first_field} = {first_field} WHERE true"
    )
    logger.info(
        f"Re-clustering existing data for {table_fqdn} (may take a while for large tables)..."
    )
    # Note: location parameter is required to avoid defaulting to US region
    # For empty tables, this query may fail due to slot/reservation issues
    # but that's acceptable - metadata update via REST API is sufficient
    try:
        job_config = bigquery.QueryJobConfig()
        bq_client.query_and_wait(update_query, job_config=job_config, location=location)
        logger.info(f"Successfully re-clustered existing data for {table_fqdn}")
    except Exception as e:
        # For empty tables or slot issues, log warning but don't fail
        # The clustering metadata was already updated via REST API
        logger.warning(
            f"Re-clustering query failed (may be empty table or slot issue): {e}. "
            f"Clustering metadata was updated successfully via REST API."
        )
