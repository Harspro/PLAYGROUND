import logging

from airflow.exceptions import AirflowException
from google.cloud.exceptions import NotFound

logger = logging.getLogger(__name__)


def _extract_table_metadata(table) -> dict:
    """
    Extract partition and clustering metadata from BigQuery Table object.

    Uses table.to_api_repr() for robustness and future-proofing.
    Returns a dict with partitioning and clustering details.

    Args:
        table: BigQuery Table object from client.get_table().

    Returns:
        dict with "partitioning" and "clustering" keys.
    """
    table_repr = table.to_api_repr() or {}
    tp = table_repr.get("timePartitioning")
    rp = table_repr.get("rangePartitioning")
    cl = table_repr.get("clustering")

    metadata = {}

    # Partitioning: timePartitioning or rangePartitioning
    if tp:
        metadata["partitioning"] = {
            "type": tp.get("type") or "DAY",
            "field": tp.get("field"),
        }
    elif rp:
        metadata["partitioning"] = {
            "type": "RANGE",
            "field": rp.get("field"),
        }
    else:
        metadata["partitioning"] = None

    # Clustering
    cluster_fields = cl.get("fields") if cl else None
    metadata["clustering"] = list(cluster_fields) if cluster_fields else None

    return metadata


def get_current_schema(client, table_ref: str):
    """
    Fetch current BigQuery table schema and metadata.

    Args:
        client:
            BigQuery client instance.
        table_ref (str):
            Fully qualified table reference.

    Returns:
        dict:
            {
                "columns": list | None,  # None when table not found (e.g., after DROP)
                "metadata": dict | None   # partition/cluster details when EXISTS
            }

    Raises:
        AirflowException:
            For unexpected errors.
    """

    try:
        table = client.get_table(table_ref)

        schema = [field.to_api_repr() for field in table.schema]

        metadata = _extract_table_metadata(table)

        return {"columns": schema, "metadata": metadata}

    except NotFound:
        return {"columns": None, "metadata": None}

    except Exception as e:
        logger.error(f"Unexpected error fetching schema for {table_ref}: {e}")
        raise AirflowException(
            f"Failed to fetch current schema for {table_ref}: {str(e)}"
        ) from e
