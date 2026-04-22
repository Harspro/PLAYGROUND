import pytest
from unittest.mock import patch, MagicMock
from airflow.exceptions import AirflowException

from bigquery_table_management.handlers.handler_factory import HandlerFactory

# ==========================================================
# SQL HANDLER RETURNED
# ==========================================================


@patch("bigquery_table_management.handlers.handler_factory.SQLHandler")
def test_get_handler_sql(mock_sql_handler):
    mock_instance = MagicMock()
    mock_sql_handler.return_value = mock_instance

    handler = HandlerFactory.get_handler(
        script_path="V1__CREATE_TABLE.sql",
        project_id="test-project",
        dataset_id="dataset",
        deploy_env="dev",
        timezone="America/Toronto",
        location="northamerica-northeast1",
    )

    mock_sql_handler.assert_called_once_with("test-project", "dataset", "dev", "America/Toronto", "northamerica-northeast1")

    assert handler == mock_instance


# ==========================================================
# SQL EXTENSION CASE INSENSITIVE
# ==========================================================


@patch("bigquery_table_management.handlers.handler_factory.SQLHandler")
def test_get_handler_sql_case_insensitive(mock_sql_handler):
    mock_instance = MagicMock()
    mock_sql_handler.return_value = mock_instance

    handler = HandlerFactory.get_handler(
        script_path="V1__CREATE_TABLE.SQL",
        project_id="test-project",
        dataset_id="dataset",
        deploy_env="dev",
        timezone="America/Toronto",
        location="northamerica-northeast1",
    )

    assert handler == mock_instance


# ==========================================================
# YAML HANDLER RETURNED
# ==========================================================


@patch("bigquery_table_management.handlers.handler_factory.YamlHandler")
def test_get_handler_yaml(mock_yaml_handler):
    mock_instance = MagicMock()
    mock_yaml_handler.return_value = mock_instance

    handler = HandlerFactory.get_handler(
        script_path="V1__CLUSTER_BY_ID.yaml",
        project_id="test-project",
        dataset_id="dataset",
        deploy_env="dev",
        timezone="America/Toronto",
        location="northamerica-northeast1",
    )

    mock_yaml_handler.assert_called_once_with("test-project", "dataset", "dev", "America/Toronto", "northamerica-northeast1")

    assert handler == mock_instance


# ==========================================================
# YAML EXTENSION CASE INSENSITIVE
# ==========================================================


@patch("bigquery_table_management.handlers.handler_factory.YamlHandler")
def test_get_handler_yaml_case_insensitive(mock_yaml_handler):
    mock_instance = MagicMock()
    mock_yaml_handler.return_value = mock_instance

    handler = HandlerFactory.get_handler(
        script_path="V1__CLUSTER_BY_ID.YAML",
        project_id="test-project",
        dataset_id="dataset",
        deploy_env="dev",
        timezone="America/Toronto",
        location="northamerica-northeast1",
    )

    assert handler == mock_instance


# ==========================================================
# UNSUPPORTED EXTENSION
# ==========================================================


def test_get_handler_unsupported_extension():
    with pytest.raises(AirflowException) as exc:
        HandlerFactory.get_handler(
            script_path="file.txt",
            project_id="test-project",
            dataset_id="dataset",
            deploy_env="dev",
            timezone="America/Toronto",
            location="northamerica-northeast1",
        )

    assert "Unsupported script type" in str(exc.value)
