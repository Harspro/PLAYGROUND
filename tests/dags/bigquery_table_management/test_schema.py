import pytest
from unittest.mock import MagicMock
from airflow.exceptions import AirflowException
from google.cloud.exceptions import NotFound

from bigquery_table_management.shared.schema import get_current_schema

# ==========================================================
# TABLE EXISTS
# ==========================================================


def test_get_current_schema_exists():
    mock_client = MagicMock()

    # Mock schema fields (to_api_repr returns BigQuery API format)
    mock_field1 = MagicMock()
    mock_field1.to_api_repr.return_value = {
        "name": "id",
        "type": "STRING",
        "mode": "REQUIRED",
    }

    mock_field2 = MagicMock()
    mock_field2.to_api_repr.return_value = {
        "name": "created_at",
        "type": "TIMESTAMP",
        "mode": "NULLABLE",
    }

    mock_table = MagicMock()
    mock_table.schema = [mock_field1, mock_field2]
    mock_table.to_api_repr.return_value = {
        "timePartitioning": {"type": "DAY", "field": "created_at"},
        "rangePartitioning": None,
        "clustering": {"fields": ["id"]},
    }

    mock_client.get_table.return_value = mock_table

    result = get_current_schema(mock_client, "project.dataset.table")

    assert result["columns"] is not None
    assert len(result["columns"]) == 2
    assert result["columns"][0] == {
        "name": "id",
        "type": "STRING",
        "mode": "REQUIRED",
    }
    assert result["columns"][1] == {
        "name": "created_at",
        "type": "TIMESTAMP",
        "mode": "NULLABLE",
    }
    assert result["metadata"] == {
        "partitioning": {"type": "DAY", "field": "created_at"},
        "clustering": ["id"],
    }


# ==========================================================
# TABLE NOT FOUND
# ==========================================================


def test_get_current_schema_not_found():
    mock_client = MagicMock()
    mock_client.get_table.side_effect = NotFound("Table not found")

    result = get_current_schema(mock_client, "project.dataset.table")

    assert result == {"columns": None, "metadata": None}


# ==========================================================
# TABLE EXISTS - NO PARTITIONING/CLUSTERING
# ==========================================================


def test_get_current_schema_exists_no_partitioning():
    mock_client = MagicMock()

    mock_field = MagicMock()
    mock_field.to_api_repr.return_value = {
        "name": "id",
        "type": "INT64",
        "mode": "REQUIRED",
    }

    mock_table = MagicMock()
    mock_table.schema = [mock_field]
    mock_table.to_api_repr.return_value = {}

    mock_client.get_table.return_value = mock_table

    result = get_current_schema(mock_client, "project.dataset.table")

    assert result["columns"] is not None
    assert result["metadata"] == {"partitioning": None, "clustering": None}


# ==========================================================
# UNEXPECTED ERROR
# ==========================================================


def test_get_current_schema_unexpected_error():
    mock_client = MagicMock()
    mock_client.get_table.side_effect = Exception("Random failure")

    with pytest.raises(AirflowException) as exc:
        get_current_schema(mock_client, "project.dataset.table")

    assert "Failed to fetch current schema" in str(exc.value)
