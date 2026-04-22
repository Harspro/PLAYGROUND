"""
Tests for BaseProcessor.

Tests the template method pattern, common workflow steps, and audit logging.
"""

import pytest
from unittest.mock import MagicMock, patch
from airflow.exceptions import AirflowException

from bigquery_table_management.processors.base_processor import BaseProcessor

# ==========================================================
# MOCK PROCESSOR FOR TESTING
# ==========================================================


class MockProcessor(BaseProcessor):
    """Concrete processor implementation for testing BaseProcessor."""

    def _do_apply(self, script_path: str, table_ref: str, version: int, config: dict) -> bool:
        """Mock implementation that returns True."""
        return True


class MockProcessorReturnsFalse(BaseProcessor):
    """Concrete processor that returns False (for skip scenarios)."""

    def _do_apply(self, script_path: str, table_ref: str, version: int, config: dict) -> bool:
        """Mock implementation that returns False."""
        return False


# ==========================================================
# FIXTURE
# ==========================================================


@pytest.fixture
def processor():
    """Create a MockProcessor instance with mocked dependencies."""
    with patch(
        "bigquery_table_management.processors.base_processor.bigquery.Client"
    ) as mock_client, patch(
        "bigquery_table_management.processors.base_processor.AuditModule"
    ) as mock_audit:
        mock_client.return_value = MagicMock()
        mock_audit.return_value = MagicMock()

        p = MockProcessor(
            project_id="test-project", dataset_id="test_dataset", deploy_env="dev", timezone="America/Toronto", location="northamerica-northeast1"
        )

        yield p


# ==========================================================
# INITIALIZATION
# ==========================================================


def test_processor_initialization(processor):
    """Test that processor initializes with correct attributes."""
    assert processor.project_id == "test-project"
    assert processor.dataset_id == "test_dataset"
    assert processor.deploy_env == "dev"
    assert processor.client is not None
    assert processor.audit is not None


# ==========================================================
# VERSION EXTRACTION
# ==========================================================


@patch(
    "bigquery_table_management.processors.base_processor.extract_version_from_filename"
)
def test_apply_extracts_version(mock_extract_version, processor):
    """Test that apply() extracts version from filename."""
    mock_extract_version.return_value = 1
    processor.audit.is_version_applied.return_value = False
    processor.audit.get_latest_version.return_value = None

    processor.apply(
        script_path="V1__OPERATION.yaml",
        table_ref="project.dataset.table",
        config={"key": "value"},
    )

    mock_extract_version.assert_called_once_with("V1__OPERATION.yaml")


# ==========================================================
# VERSION-BASED IDEMPOTENCY CHECK
# ==========================================================


@patch(
    "bigquery_table_management.processors.base_processor.extract_version_from_filename"
)
def test_apply_skips_if_version_already_applied(mock_extract_version, processor):
    """Test that apply() skips if version already applied."""
    mock_extract_version.return_value = 3
    processor.audit.is_version_applied.return_value = True

    result = processor.apply(
        script_path="V3__OPERATION.yaml",
        table_ref="project.dataset.table",
        config={"key": "value"},
    )

    assert result is False
    processor.audit.is_version_applied.assert_called_once_with(
        "project.dataset.table", 3
    )
    # Should not call _do_apply if already applied
    # (We can't directly verify this, but we can verify audit.get_latest_version wasn't called)
    processor.audit.get_latest_version.assert_not_called()


# ==========================================================
# RUNTIME VERSION INCREMENT VALIDATION
# ==========================================================


@patch(
    "bigquery_table_management.processors.base_processor.validate_runtime_version_increment"
)
@patch(
    "bigquery_table_management.processors.base_processor.extract_version_from_filename"
)
def test_apply_validates_version_increment(
    mock_extract_version, mock_validate_version, processor
):
    """Test that apply() validates version increment."""
    mock_extract_version.return_value = 4
    processor.audit.is_version_applied.return_value = False
    processor.audit.get_latest_version.return_value = 2

    processor.apply(
        script_path="V4__OPERATION.yaml",
        table_ref="project.dataset.table",
        config={"key": "value"},
    )

    mock_validate_version.assert_called_once_with(new_version=4, last_applied_version=2)


@patch(
    "bigquery_table_management.processors.base_processor.validate_runtime_version_increment"
)
@patch(
    "bigquery_table_management.processors.base_processor.extract_version_from_filename"
)
def test_apply_raises_on_version_validation_failure(
    mock_extract_version, mock_validate_version, processor
):
    """Test that apply() raises exception on version validation failure."""
    mock_extract_version.return_value = 3
    processor.audit.is_version_applied.return_value = False
    processor.audit.get_latest_version.return_value = 1
    mock_validate_version.side_effect = AirflowException("Version jump too large")

    with pytest.raises(AirflowException):
        processor.apply(
            script_path="V3__OPERATION.yaml",
            table_ref="project.dataset.table",
            config={"key": "value"},
        )


# ==========================================================
# PROCESSOR-SPECIFIC WORK EXECUTION
# ==========================================================


@patch(
    "bigquery_table_management.processors.base_processor.extract_version_from_filename"
)
def test_apply_calls_do_apply(mock_extract_version, processor):
    """Test that apply() calls _do_apply with correct parameters."""
    mock_extract_version.return_value = 1
    processor.audit.is_version_applied.return_value = False
    processor.audit.get_latest_version.return_value = None

    config = {"clustering_fields": ["field1"]}

    result = processor.apply(
        script_path="V1__OPERATION.yaml",
        table_ref="project.dataset.table",
        config=config,
    )

    assert result is True
    # _do_apply is called internally, we verify it was called by checking result


# ==========================================================
# AUDIT LOGGING (ONLY IF EXECUTED)
# ==========================================================


@patch(
    "bigquery_table_management.processors.base_processor.extract_version_from_filename"
)
def test_apply_logs_to_audit_when_executed(
    mock_extract_version, processor
):
    """Test that apply() logs to audit when operation is executed."""
    mock_extract_version.return_value = 1
    processor.audit.is_version_applied.return_value = False
    processor.audit.get_latest_version.return_value = None

    config = {"key": "value"}

    result = processor.apply(
        script_path="V1__OPERATION.yaml",
        table_ref="project.dataset.table",
        config=config,
    )

    assert result is True
    # Verify log_execution was called with correct parameters
    processor.audit.log_execution.assert_called_once_with(
        table_ref="project.dataset.table",
        version=1,
        script_path="V1__OPERATION.yaml",
        bq_client=processor.client,
    )


@patch(
    "bigquery_table_management.processors.base_processor.extract_version_from_filename"
)
def test_apply_does_not_log_when_skipped(mock_extract_version, processor):
    """Test that apply() does not log to audit when operation is skipped."""
    mock_extract_version.return_value = 1
    processor.audit.is_version_applied.return_value = True

    result = processor.apply(
        script_path="V1__OPERATION.yaml",
        table_ref="project.dataset.table",
        config={"key": "value"},
    )

    assert result is False
    processor.audit.log_execution.assert_not_called()


@patch(
    "bigquery_table_management.processors.base_processor.extract_version_from_filename"
)
def test_apply_does_not_log_when_do_apply_returns_false(
    mock_extract_version, processor
):
    """Test that apply() does not log when _do_apply returns False."""
    # Use MockProcessorReturnsFalse which returns False
    with patch(
        "bigquery_table_management.processors.base_processor.bigquery.Client"
    ) as mock_client, patch(
        "bigquery_table_management.processors.base_processor.AuditModule"
    ) as mock_audit:
        mock_client.return_value = MagicMock()
        mock_audit.return_value = MagicMock()

        p = MockProcessorReturnsFalse(
            project_id="test-project", dataset_id="test_dataset", deploy_env="dev", timezone="America/Toronto", location="northamerica-northeast1"
        )

        mock_extract_version.return_value = 1
        p.audit.is_version_applied.return_value = False
        p.audit.get_latest_version.return_value = None

        result = p.apply(
            script_path="V1__OPERATION.yaml",
            table_ref="project.dataset.table",
            config={"key": "value"},
        )

        assert result is False
        p.audit.log_execution.assert_not_called()


# ==========================================================
# ABSTRACT METHOD ENFORCEMENT
# ==========================================================


def test_base_processor_is_abstract():
    """Test that BaseProcessor cannot be instantiated directly."""
    with patch(
        "bigquery_table_management.processors.base_processor.bigquery.Client"
    ) as mock_client, patch(
        "bigquery_table_management.processors.base_processor.AuditModule"
    ) as mock_audit:
        mock_client.return_value = MagicMock()
        mock_audit.return_value = MagicMock()

        # BaseProcessor should raise TypeError if we try to call abstract method
        # But actually, Python's ABC allows instantiation if abstract methods aren't called
        # So we verify that a subclass without _do_apply cannot be instantiated
        class IncompleteProcessor(BaseProcessor):
            pass

        with pytest.raises(TypeError):
            IncompleteProcessor(
                project_id="test-project", dataset_id="test_dataset", deploy_env="dev", timezone="America/Toronto", location="northamerica-northeast1"
            )
