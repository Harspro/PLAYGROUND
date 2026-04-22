"""
Tests for ClusteringProcessor.

Tests clustering update logic, state-based idempotency, and error handling.
"""

import pytest
from unittest.mock import MagicMock, patch
from airflow.exceptions import AirflowException

from bigquery_table_management.processors.clustering.clustering_processor import (
    ClusteringProcessor,
)

# ==========================================================
# FIXTURE
# ==========================================================


@pytest.fixture
def processor():
    """Create a ClusteringProcessor instance with mocked dependencies."""
    with patch(
        "bigquery_table_management.processors.base_processor.bigquery.Client"
    ) as mock_client, patch(
        "bigquery_table_management.processors.base_processor.AuditModule"
    ) as mock_audit:
        mock_client.return_value = MagicMock()
        mock_audit.return_value = MagicMock()

        p = ClusteringProcessor(
            project_id="test-project", dataset_id="test_dataset", deploy_env="dev", timezone="America/Toronto", location="northamerica-northeast1"
        )

        yield p


# ==========================================================
# SUCCESSFUL CLUSTERING UPDATE
# ==========================================================


@patch(
    "bigquery_table_management.processors.clustering.clustering_processor.update_table_clustering"
)
@patch(
    "bigquery_table_management.processors.clustering.clustering_processor.get_current_clustering"
)
def test_do_apply_successful_update(mock_get_clustering, mock_update_clustering, processor):
    """Test successful clustering update when current differs from desired."""
    mock_get_clustering.return_value = ["old_field"]
    config = {"clustering_fields": ["field1", "field2"]}

    result = processor._do_apply(
        script_path="V1__CLUSTER.yaml",
        table_ref="project.dataset.table",
        version=1,
        config=config,
    )

    assert result is True
    mock_get_clustering.assert_called_once_with("project.dataset.table")
    mock_update_clustering.assert_called_once_with(
        table_fqdn="project.dataset.table",
        clustering_fields=["field1", "field2"],
        bq_client=processor.client,
        location="northamerica-northeast1",
    )


# ==========================================================
# STATE-BASED IDEMPOTENCY (ALREADY CLUSTERED)
# ==========================================================


@patch(
    "bigquery_table_management.shared.audit.get_current_schema"
)
@patch(
    "bigquery_table_management.processors.clustering.clustering_processor.get_current_clustering"
)
@patch(
    "bigquery_table_management.processors.clustering.clustering_processor.update_table_clustering"
)
def test_do_apply_skips_when_already_clustered(
    mock_update_clustering,
    mock_get_clustering,
    mock_get_schema,
    processor,
):
    """Test that clustering update is skipped when table already has desired clustering."""
    mock_get_clustering.return_value = ["field1", "field2"]
    mock_get_schema.return_value = {
        "columns": [{"name": "field1", "type": "STRING"}],
        "metadata": {"clustering": {"fields": ["field1", "field2"]}},
    }

    config = {"clustering_fields": ["field1", "field2"]}

    result = processor._do_apply(
        script_path="V1__CLUSTER.yaml",
        table_ref="project.dataset.table",
        version=1,
        config=config,
    )

    assert result is False
    mock_get_clustering.assert_called_once_with("project.dataset.table")
    mock_update_clustering.assert_not_called()
    # Should still log to audit for tracking
    processor.audit.log_execution.assert_called_once()


# ==========================================================
# MISSING CLUSTERING_FIELDS
# ==========================================================


def test_do_apply_missing_clustering_fields(processor):
    """Test that missing clustering_fields raises error."""
    config = {}

    with pytest.raises(AirflowException) as exc:
        processor._do_apply(
            script_path="V1__CLUSTER.yaml",
            table_ref="project.dataset.table",
            version=1,
            config=config,
        )

    assert "No clustering_fields found" in str(exc.value)


@patch(
    "bigquery_table_management.processors.clustering.clustering_processor.update_table_clustering"
)
@patch(
    "bigquery_table_management.processors.clustering.clustering_processor.get_current_clustering"
)
def test_do_apply_empty_clustering_fields_removes_clustering(
    mock_get_clustering, mock_update_clustering, processor
):
    """Test that empty clustering_fields list triggers remove-clustering path (no error)."""
    mock_get_clustering.return_value = ["old_field"]
    config = {"clustering_fields": []}

    result = processor._do_apply(
        script_path="V1__REMOVE_CLUSTERING.yaml",
        table_ref="project.dataset.table",
        version=1,
        config=config,
    )

    assert result is True
    mock_get_clustering.assert_called_once_with("project.dataset.table")
    mock_update_clustering.assert_called_once_with(
        table_fqdn="project.dataset.table",
        clustering_fields=[],
        bq_client=processor.client,
        location="northamerica-northeast1",
    )


@patch(
    "bigquery_table_management.processors.clustering.clustering_processor.update_table_clustering"
)
@patch(
    "bigquery_table_management.processors.clustering.clustering_processor.get_current_clustering"
)
def test_do_apply_empty_clustering_fields_skips_when_already_empty(
    mock_get_clustering, mock_update_clustering, processor
):
    """Test that empty clustering_fields skips when table already has no clustering (state-based idempotency)."""
    mock_get_clustering.return_value = []
    config = {"clustering_fields": []}

    result = processor._do_apply(
        script_path="V1__REMOVE_CLUSTERING.yaml",
        table_ref="project.dataset.table",
        version=1,
        config=config,
    )

    assert result is False
    mock_get_clustering.assert_called_once_with("project.dataset.table")
    mock_update_clustering.assert_not_called()
    processor.audit.log_execution.assert_called_once()


# ==========================================================
# ENVIRONMENT PLACEHOLDER REPLACEMENT
# ==========================================================


@patch(
    "bigquery_table_management.processors.clustering.clustering_processor.update_table_clustering"
)
@patch(
    "bigquery_table_management.processors.clustering.clustering_processor.get_current_clustering"
)
def test_do_apply_replaces_env_placeholders(
    mock_get_clustering, mock_update_clustering, processor
):
    """Test that {env} placeholders are replaced in clustering field names."""
    processor.deploy_env = "prod"
    mock_get_clustering.return_value = []
    config = {"clustering_fields": ["field_{env}", "other_{env}_field"]}

    result = processor._do_apply(
        script_path="V1__CLUSTER.yaml",
        table_ref="project.dataset.table",
        version=1,
        config=config,
    )

    assert result is True
    mock_update_clustering.assert_called_once_with(
        table_fqdn="project.dataset.table",
        clustering_fields=["field_prod", "other_prod_field"],
        bq_client=processor.client,
        location="northamerica-northeast1",
    )


# ==========================================================
# INTEGRATION WITH BASE PROCESSOR
# ==========================================================


@patch(
    "bigquery_table_management.processors.clustering.clustering_processor.update_table_clustering"
)
@patch(
    "bigquery_table_management.processors.clustering.clustering_processor.get_current_clustering"
)
@patch(
    "bigquery_table_management.shared.audit.get_current_schema"
)
@patch(
    "bigquery_table_management.processors.base_processor.extract_version_from_filename"
)
def test_apply_integration_with_base_processor(
    mock_extract_version,
    mock_get_schema,
    mock_get_clustering,
    mock_update_clustering,
    processor,
):
    """Test full apply() workflow including BaseProcessor common steps."""
    # Setup mocks
    mock_extract_version.return_value = 2
    processor.audit.is_version_applied.return_value = False
    processor.audit.get_latest_version.return_value = 1
    mock_get_clustering.return_value = []
    mock_get_schema.return_value = {
        "columns": [],
        "metadata": {"clustering": {"fields": ["field1"]}},
    }

    config = {"clustering_fields": ["field1"]}

    result = processor.apply(
        script_path="V2__CLUSTER.yaml",
        table_ref="project.dataset.table",
        config=config,
    )

    assert result is True
    # Verify BaseProcessor workflow
    processor.audit.is_version_applied.assert_called_once_with(
        "project.dataset.table", 2
    )
    processor.audit.get_latest_version.assert_called_once_with("project.dataset.table")
    # Verify ClusteringProcessor-specific logic
    mock_get_clustering.assert_called_once()
    mock_update_clustering.assert_called_once()
    # Verify audit logging
    processor.audit.log_execution.assert_called_once()


@patch(
    "bigquery_table_management.processors.clustering.clustering_processor.get_current_clustering"
)
@patch(
    "bigquery_table_management.processors.base_processor.extract_version_from_filename"
)
def test_apply_skips_if_version_already_applied(
    mock_extract_version, mock_get_clustering, processor
):
    """Test that apply() skips if version already applied (BaseProcessor logic)."""
    mock_extract_version.return_value = 1
    processor.audit.is_version_applied.return_value = True

    config = {"clustering_fields": ["field1"]}

    result = processor.apply(
        script_path="V1__CLUSTER.yaml",
        table_ref="project.dataset.table",
        config=config,
    )

    assert result is False
    mock_get_clustering.assert_not_called()
    processor.audit.log_execution.assert_not_called()
