import pytest
from unittest.mock import MagicMock, patch
from airflow.exceptions import AirflowException

from bigquery_table_management.handlers.sql_handler import SQLHandler

# ==========================================================
# FIXTURE
# ==========================================================


@pytest.fixture
def handler():
    """Create a SQLHandler instance with mocked dependencies."""
    h = SQLHandler(
        project_id="test-project", dataset_id="test_dataset", deploy_env="dev", timezone="America/Toronto", location="northamerica-northeast1"
    )
    h._read_sql = MagicMock()
    yield h


# ==========================================================
# ROUTING TO SQL PROCESSOR
# ==========================================================


@patch("bigquery_table_management.handlers.sql_handler.SQLProcessor")
def test_execute_routes_to_sql_processor(mock_sql_processor, handler):
    """Test that SQLHandler routes to SQLProcessor."""
    mock_processor_instance = MagicMock()
    mock_processor_instance.apply.return_value = True
    mock_sql_processor.return_value = mock_processor_instance

    handler._read_sql.return_value = "CREATE TABLE project.dataset.table (id INT64)"

    result = handler.execute(
        script_path="V1__CREATE.sql", table_ref="project.dataset.table"
    )

    assert result is True
    # Verify SQLProcessor was instantiated
    mock_sql_processor.assert_called_once()
    call_args = mock_sql_processor.call_args[0]
    assert call_args == ("test-project", "test_dataset", "dev", "America/Toronto", "northamerica-northeast1")
    # Verify SQL was read
    handler._read_sql.assert_called_once_with("V1__CREATE.sql")
    # Verify processor was called with SQL in config
    mock_processor_instance.apply.assert_called_once()
    apply_call_args = mock_processor_instance.apply.call_args
    assert apply_call_args[1]["config"] == {"sql": "CREATE TABLE project.dataset.table (id INT64)"}


# ==========================================================
# ENVIRONMENT PLACEHOLDER REPLACEMENT
# ==========================================================


@patch("bigquery_table_management.handlers.sql_handler.SQLProcessor")
def test_execute_replaces_env_placeholders(mock_sql_processor, handler):
    """Test that {env} placeholders are replaced in SQL."""
    handler.deploy_env = "prod"
    mock_processor_instance = MagicMock()
    mock_processor_instance.apply.return_value = True
    mock_sql_processor.return_value = mock_processor_instance

    handler._read_sql.return_value = "CREATE TABLE pcb-{env}-landing.dataset.table (id INT64)"

    result = handler.execute(
        script_path="V1__CREATE.sql", table_ref="project.dataset.table"
    )

    assert result is True
    # Verify environment was replaced
    mock_processor_instance.apply.assert_called_once()
    apply_call_args = mock_processor_instance.apply.call_args
    assert apply_call_args[1]["config"] == {"sql": "CREATE TABLE pcb-prod-landing.dataset.table (id INT64)"}


# ==========================================================
# READ SQL FILE FAILURE
# ==========================================================


def test_read_sql_file_not_found():
    """Test that file not found raises error."""
    # Create handler without mocking _read_sql
    handler = SQLHandler(
        project_id="test-project", dataset_id="test_dataset", deploy_env="dev", timezone="America/Toronto", location="northamerica-northeast1"
    )
    with patch("builtins.open", side_effect=FileNotFoundError("No such file")):
        with pytest.raises(AirflowException):
            handler._read_sql("invalid_path.sql")
