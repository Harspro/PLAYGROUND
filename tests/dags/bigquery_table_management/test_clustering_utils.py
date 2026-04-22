import pytest
from unittest.mock import MagicMock, patch
from google.api_core.exceptions import NotFound

from bigquery_table_management.processors.clustering.clustering_utils import (
    get_current_clustering,
    update_table_clustering,
)
from util.bq_utils import BigQueryRestApiError


# ==========================================================
# get_current_clustering
# ==========================================================


@patch("bigquery_table_management.processors.clustering.clustering_utils.get_table_via_rest")
def test_get_current_clustering_success(mock_get_rest):
    """Test successful retrieval of clustering fields."""
    mock_get_rest.return_value = {"clustering": {"fields": ["field1", "field2"]}}
    result = get_current_clustering("project.dataset.table")
    assert result == ["field1", "field2"]


@patch("bigquery_table_management.processors.clustering.clustering_utils.get_table_via_rest")
def test_get_current_clustering_no_clustering(mock_get_rest):
    """Test when table exists but has no clustering."""
    mock_get_rest.return_value = {"id": "some-id"}  # No clustering key
    result = get_current_clustering("project.dataset.table")
    assert result is None


@patch("bigquery_table_management.processors.clustering.clustering_utils.get_table_via_rest")
def test_get_current_clustering_empty_fields(mock_get_rest):
    """Test when clustering key exists but fields are empty."""
    mock_get_rest.return_value = {"clustering": {"fields": []}}
    result = get_current_clustering("project.dataset.table")
    assert result is None


@patch("bigquery_table_management.processors.clustering.clustering_utils.get_table_via_rest")
def test_get_current_clustering_not_found(mock_get_rest):
    """Test when table is not found."""
    mock_get_rest.side_effect = NotFound("Table not found")
    result = get_current_clustering("project.dataset.table")
    assert result is None


@patch("bigquery_table_management.processors.clustering.clustering_utils.get_table_via_rest")
def test_get_current_clustering_api_error(mock_get_rest):
    """Test when REST API fails."""
    mock_get_rest.side_effect = BigQueryRestApiError("API Error", 500)
    with pytest.raises(BigQueryRestApiError):
        get_current_clustering("project.dataset.table")


# ==========================================================
# update_table_clustering
# ==========================================================


@patch("bigquery_table_management.processors.clustering.clustering_utils.patch_table_via_rest")
def test_update_table_clustering_remove(mock_patch_rest):
    """Test removing clustering (empty fields)."""
    mock_client = MagicMock()
    update_table_clustering(
        table_fqdn="project.dataset.table",
        clustering_fields=[],
        bq_client=mock_client,
        location="northamerica-northeast1"
    )
    mock_patch_rest.assert_called_once_with("project.dataset.table", {"clustering": None})
    mock_client.query_and_wait.assert_not_called()


@patch("bigquery_table_management.processors.clustering.clustering_utils.patch_table_via_rest")
def test_update_table_clustering_success(mock_patch_rest):
    """Test successful clustering update and re-clustering query."""
    mock_client = MagicMock()
    update_table_clustering(
        table_fqdn="project.dataset.table",
        clustering_fields=["field1"],
        bq_client=mock_client,
        location="northamerica-northeast1"
    )
    mock_patch_rest.assert_called_once_with(
        "project.dataset.table", {"clustering": {"fields": ["field1"]}}
    )
    mock_client.query_and_wait.assert_called_once()
    query = mock_client.query_and_wait.call_args[0][0]
    assert "UPDATE `project.dataset.table` SET field1 = field1 WHERE true" in query


@patch("bigquery_table_management.processors.clustering.clustering_utils.patch_table_via_rest")
def test_update_table_clustering_query_fails_gracefully(mock_patch_rest):
    """Test that query failure (e.g. empty table) doesn't fail the whole update."""
    mock_client = MagicMock()
    mock_client.query_and_wait.side_effect = Exception("Query failed")

    # Should not raise exception
    update_table_clustering(
        table_fqdn="project.dataset.table",
        clustering_fields=["field1"],
        bq_client=mock_client,
        location="northamerica-northeast1"
    )
    mock_patch_rest.assert_called_once()
    mock_client.query_and_wait.assert_called_once()
