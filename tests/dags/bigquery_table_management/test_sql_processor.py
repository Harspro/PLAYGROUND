import pytest
from unittest.mock import MagicMock, patch
from airflow.exceptions import AirflowException
from google.api_core.exceptions import BadRequest

from bigquery_table_management.processors.sql.sql_processor import SQLProcessor


@pytest.fixture
def processor():
    """Create a SQLProcessor instance with mocked dependencies."""
    with patch(
        "bigquery_table_management.processors.base_processor.bigquery.Client"
    ) as mock_client, patch(
        "bigquery_table_management.processors.base_processor.AuditModule"
    ) as mock_audit:
        mock_client.return_value = MagicMock()
        mock_audit.return_value = MagicMock()

        p = SQLProcessor(
            project_id="test-project",
            dataset_id="test_dataset",
            deploy_env="dev",
            timezone="America/Toronto",
            location="northamerica-northeast1"
        )
        yield p


def test_do_apply_success(processor):
    """Test successful SQL execution workflow."""
    config = {"sql": "CREATE TABLE project.dataset.table (id INT64)"}

    with patch.object(processor, "_dry_run") as mock_dry_run, \
         patch.object(processor, "_execute") as mock_execute:

        result = processor._do_apply(
            script_path="V1__CREATE.sql",
            table_ref="project.dataset.table",
            version=1,
            config=config
        )

        assert result is True
        mock_dry_run.assert_called_once_with(config["sql"])
        mock_execute.assert_called_once_with(config["sql"])


def test_do_apply_missing_sql(processor):
    """Test that missing SQL in config raises error."""
    config = {}
    with pytest.raises(AirflowException, match="No SQL content found"):
        processor._do_apply(
            script_path="V1__CREATE.sql",
            table_ref="project.dataset.table",
            version=1,
            config=config
        )


def test_dry_run_success(processor):
    """Test successful dry run."""
    sql = "CREATE TABLE project.dataset.table (id INT64)"
    processor._dry_run(sql)
    processor.client.query_and_wait.assert_called_once()
    args, kwargs = processor.client.query_and_wait.call_args
    assert args[0] == sql
    assert kwargs["job_config"].dry_run is True


def test_dry_run_bad_request(processor):
    """Test dry run failing with BadRequest."""
    processor.client.query_and_wait.side_effect = BadRequest("Invalid SQL")
    with pytest.raises(AirflowException, match="Dry run validation failed"):
        processor._dry_run("INVALID SQL")


def test_dry_run_unexpected_error(processor):
    """Test dry run failing with unexpected error."""
    processor.client.query_and_wait.side_effect = Exception("Network error")
    with pytest.raises(AirflowException, match="Unexpected error during dry run"):
        processor._dry_run("CREATE TABLE ...")


def test_execute_success(processor):
    """Test successful execution."""
    sql = "CREATE TABLE project.dataset.table (id INT64)"
    processor._execute(sql)
    processor.client.query_and_wait.assert_called_once()
    args, kwargs = processor.client.query_and_wait.call_args
    assert args[0] == sql
    # dry_run defaults to None in QueryJobConfig if not set, which BQ treats as False
    assert kwargs["job_config"].dry_run in (False, None)


def test_execute_failure(processor):
    """Test execution failure."""
    processor.client.query_and_wait.side_effect = Exception("Execution failed")
    with pytest.raises(AirflowException, match="SQL execution failed"):
        processor._execute("CREATE TABLE ...")
