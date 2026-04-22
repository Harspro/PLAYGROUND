"""
Tests for YamlHandler.

Tests YAML file reading, operation type detection, and routing to processors.
"""

import pytest
from unittest.mock import MagicMock, patch, mock_open
from airflow.exceptions import AirflowException

from bigquery_table_management.handlers.yaml_handler import YamlHandler

# ==========================================================
# FIXTURE
# ==========================================================


@pytest.fixture
def handler():
    """Create a YamlHandler instance with mocked dependencies."""
    with patch(
        "bigquery_table_management.handlers.yaml_handler.read_yamlfile_env"
    ):
        h = YamlHandler(
            project_id="test-project", dataset_id="test_dataset", deploy_env="dev", timezone="America/Toronto", location="northamerica-northeast1"
        )
        h._read_yaml_config = MagicMock()
        yield h


# ==========================================================
# CLUSTERING OPERATION ROUTING
# ==========================================================


@patch("bigquery_table_management.handlers.yaml_handler.ClusteringProcessor")
def test_execute_routes_to_clustering_processor(mock_clustering_processor, handler):
    """Test that clustering_fields routes to ClusteringProcessor."""
    mock_processor_instance = MagicMock()
    mock_processor_instance.apply.return_value = True
    mock_clustering_processor.return_value = mock_processor_instance

    handler._read_yaml_config.return_value = {
        "clustering_fields": ["field1", "field2"]
    }

    result = handler.execute(
        script_path="V1__CLUSTER.yaml", table_ref="project.dataset.table"
    )

    assert result is True
    # Verify ClusteringProcessor was instantiated
    mock_clustering_processor.assert_called_once()
    call_args = mock_clustering_processor.call_args[0]
    assert call_args == ("test-project", "test_dataset", "dev", "America/Toronto", "northamerica-northeast1")
    # Verify apply was called with correct args
    mock_processor_instance.apply.assert_called_once()
    apply_call_args = mock_processor_instance.apply.call_args
    assert apply_call_args[1]["config"] == {"clustering_fields": ["field1", "field2"]}


# ==========================================================
# PARTITION OPERATION (NOT IMPLEMENTED)
# ==========================================================


def test_execute_partition_not_implemented(handler):
    """Test that partition_field raises not implemented error."""
    handler._read_yaml_config.return_value = {"partition_field": "date_field"}

    with pytest.raises(AirflowException) as exc:
        handler.execute(
            script_path="V1__PARTITION.yaml", table_ref="project.dataset.table"
        )

    assert "Partitioning handler not implemented" in str(exc.value)
    assert "partition_field" in str(exc.value)


# ==========================================================
# TRANSFORMATIONS OPERATION (NOT IMPLEMENTED)
# ==========================================================


def test_execute_transformations_not_implemented(handler):
    """Test that transformations raises not implemented error."""
    handler._read_yaml_config.return_value = {"transformations": []}

    with pytest.raises(AirflowException) as exc:
        handler.execute(
            script_path="V1__TRANSFORM.yaml", table_ref="project.dataset.table"
        )

    assert "Schema evolution handler not implemented" in str(exc.value)
    assert "transformations" in str(exc.value)


# ==========================================================
# UNKNOWN OPERATION TYPE
# ==========================================================


def test_execute_unknown_operation_type(handler):
    """Test that unknown YAML content raises error."""
    handler._read_yaml_config.return_value = {"unknown_key": "value"}

    with pytest.raises(AirflowException) as exc:
        handler.execute(
            script_path="V1__UNKNOWN.yaml", table_ref="project.dataset.table"
        )

    assert "Unable to detect operation type" in str(exc.value)
    assert "clustering_fields, partition_field, transformations" in str(exc.value)


# ==========================================================
# YAML READING
# ==========================================================


@patch("bigquery_table_management.handlers.yaml_handler.read_yamlfile_env")
@patch("bigquery_table_management.handlers.yaml_handler.os.path.join")
def test_read_yaml_config_success(mock_join, mock_read_yaml):
    """Test successful YAML config reading."""
    mock_join.return_value = "/path/to/config.yaml"
    mock_read_yaml.return_value = {"clustering_fields": ["field1"]}

    handler = YamlHandler(
        project_id="test-project", dataset_id="test_dataset", deploy_env="dev", timezone="America/Toronto", location="northamerica-northeast1"
    )

    result = handler._read_yaml_config("schemas/test/V1__CLUSTER.yaml")

    assert result == {"clustering_fields": ["field1"]}
    mock_join.assert_called_once()
    mock_read_yaml.assert_called_once_with("/path/to/config.yaml", deploy_env="dev")


@patch("bigquery_table_management.handlers.yaml_handler.read_yamlfile_env")
@patch("bigquery_table_management.handlers.yaml_handler.os.path.join")
def test_read_yaml_config_file_not_found(mock_join, mock_read_yaml):
    """Test YAML config file not found error."""
    mock_join.return_value = "/path/to/config.yaml"
    mock_read_yaml.return_value = None

    handler = YamlHandler(
        project_id="test-project", dataset_id="test_dataset", deploy_env="dev", timezone="America/Toronto", location="northamerica-northeast1"
    )

    with pytest.raises(AirflowException) as exc:
        handler._read_yaml_config("schemas/test/V1__CLUSTER.yaml")

    assert "YAML config file not found" in str(exc.value)


@patch("bigquery_table_management.handlers.yaml_handler.read_yamlfile_env")
@patch("bigquery_table_management.handlers.yaml_handler.os.path.join")
def test_read_yaml_config_invalid_content(mock_join, mock_read_yaml):
    """Test YAML config with invalid content (not a dict)."""
    mock_join.return_value = "/path/to/config.yaml"
    mock_read_yaml.return_value = "not a dict"

    handler = YamlHandler(
        project_id="test-project", dataset_id="test_dataset", deploy_env="dev", timezone="America/Toronto", location="northamerica-northeast1"
    )

    with pytest.raises(AirflowException) as exc:
        handler._read_yaml_config("schemas/test/V1__CLUSTER.yaml")

    assert "YAML file is empty or invalid" in str(exc.value)
