import pytest
from unittest.mock import MagicMock, patch
from airflow.exceptions import AirflowException

from bigquery_table_management.shared.audit import AuditModule

# ==========================================================
# FIXTURE
# ==========================================================


@pytest.fixture
def audit():
    with patch("bigquery_table_management.shared.audit.bigquery.Client") as mock_client:
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance
        return AuditModule(project_id="test-project", timezone="America/Toronto")


# ==========================================================
# ENSURE TABLE EXISTS
# ==========================================================


@patch("bigquery_table_management.shared.audit.bigquery.Table")
@patch("bigquery_table_management.shared.audit.bigquery.SchemaField")
@patch("bigquery_table_management.shared.audit.bigquery.TimePartitioning")
def test_ensure_table_exists_success(
    mock_partition,
    mock_schema_field,
    mock_table,
):
    with patch("bigquery_table_management.shared.audit.bigquery.Client") as mock_client:
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance

        AuditModule(project_id="test-project", timezone="America/Toronto")

        mock_instance.create_table.assert_called_once()


def test_ensure_table_exists_failure():
    with patch("bigquery_table_management.shared.audit.bigquery.Client") as mock_client:
        mock_instance = MagicMock()
        mock_instance.create_table.side_effect = Exception("Create failed")
        mock_client.return_value = mock_instance

        with pytest.raises(AirflowException):
            AuditModule(project_id="test-project", timezone="America/Toronto")


# ==========================================================
# RUN QUERY
# ==========================================================


def test_run_query_success(audit):
    mock_result = MagicMock()
    audit.client.query_and_wait.return_value = mock_result

    result = audit._run_query("SELECT 1", [])

    assert result == mock_result
    audit.client.query_and_wait.assert_called_once()


def test_run_query_failure(audit):
    audit.client.query_and_wait.side_effect = Exception("Query failed")

    with pytest.raises(AirflowException):
        audit._run_query("SELECT 1", [])


# ==========================================================
# IS VERSION APPLIED
# ==========================================================


def test_is_version_applied_true(audit):
    mock_row = MagicMock()
    mock_row.cnt = 1
    audit._run_query = MagicMock(return_value=[mock_row])

    result = audit.is_version_applied(table_ref="project.dataset.table", version=1)

    assert result is True


def test_is_version_applied_false(audit):
    mock_row = MagicMock()
    mock_row.cnt = 0
    audit._run_query = MagicMock(return_value=[mock_row])

    result = audit.is_version_applied(table_ref="project.dataset.table", version=1)

    assert result is False


# ==========================================================
# GET LATEST VERSION
# ==========================================================


def test_get_latest_version_found(audit):
    mock_row = MagicMock()
    mock_row.version = 5
    audit._run_query = MagicMock(return_value=[mock_row])

    result = audit.get_latest_version("project.dataset.table")

    assert result == 5


def test_get_latest_version_none(audit):
    audit._run_query = MagicMock(return_value=[])

    result = audit.get_latest_version("project.dataset.table")

    assert result is None


# ==========================================================
# LOG EXECUTION
# ==========================================================


@patch("bigquery_table_management.shared.audit.get_current_schema")
def test_log_execution_success(mock_get_schema, audit):
    audit._run_query = MagicMock()
    mock_get_schema.return_value = {
        "columns": [],
        "metadata": {
            "partitioning": {"type": "DAY", "field": "created_at"},
            "clustering": None,
        },
    }
    mock_client = MagicMock()

    audit.log_execution(
        table_ref="project.dataset.table",
        version=1,
        script_path="V1__CREATE.sql",
        bq_client=mock_client,
    )

    mock_get_schema.assert_called_once_with(mock_client, "project.dataset.table")
    audit._run_query.assert_called_once()


@patch("bigquery_table_management.shared.audit.get_current_schema")
def test_log_execution_failure(mock_get_schema, audit):
    audit._run_query = MagicMock(side_effect=AirflowException("Insert failed"))
    mock_get_schema.return_value = {"columns": None, "metadata": None}
    mock_client = MagicMock()

    with pytest.raises(AirflowException):
        audit.log_execution(
            table_ref="project.dataset.table",
            version=1,
            script_path="V1__CREATE.sql",
            bq_client=mock_client,
        )
