"""
Unit tests for BigQueryTableManagementDagBuilder.
"""

import pytest
from unittest.mock import MagicMock, patch
from airflow.exceptions import AirflowException
import pendulum

from bigquery_table_management.bigquery_table_management_dag import (
    BigQueryTableManagementDagBuilder,
)

# Matches util.constants.LANDING_ZONE_PROJECT_ID
LANDING_ZONE_PROJECT_ID_KEY = "landing_zone_project_id"

# ==========================================================
# FIXTURE
# ==========================================================


@pytest.fixture
def mock_environment_config():
    config = MagicMock()
    config.gcp_config = {LANDING_ZONE_PROJECT_ID_KEY: "default-project"}
    config.deploy_env = "dev"
    config.local_tz = pendulum.timezone("America/Toronto")
    return config


@pytest.fixture
def builder(mock_environment_config):
    return BigQueryTableManagementDagBuilder(mock_environment_config)


# ==========================================================
# _extract_dataset_id
# ==========================================================


def test_extract_dataset_id_project_dataset_table(builder):
    assert builder._extract_dataset_id("project.dataset.table") == "dataset"


def test_extract_dataset_id_dataset_table(builder):
    assert builder._extract_dataset_id("dataset.table") == "dataset"


def test_extract_dataset_id_invalid_format(builder):
    with pytest.raises(AirflowException) as exc:
        builder._extract_dataset_id("invalid")
    assert "Invalid table_ref format" in str(exc.value)


def test_extract_dataset_id_single_part(builder):
    with pytest.raises(AirflowException):
        builder._extract_dataset_id("onlyonepart")


# ==========================================================
# _extract_project_id
# ==========================================================


def test_extract_project_id_project_dataset_table(builder):
    assert builder._extract_project_id("project.dataset.table") == "project"


def test_extract_project_id_dataset_table_uses_default(builder):
    assert builder._extract_project_id("dataset.table") == "default-project"


def test_extract_project_id_invalid_format(builder):
    with pytest.raises(AirflowException) as exc:
        builder._extract_project_id("invalid")
    assert "Invalid table_ref format" in str(exc.value)


# ==========================================================
# build
# ==========================================================


@patch(
    "bigquery_table_management.bigquery_table_management_dag.BigQueryTableManagementDagBuilder.prepare_default_args"
)
def test_build_success(mock_prepare, builder):
    mock_prepare.return_value = {}

    config = {
        "table_ref": "project.dataset.table",
        "scripts": ["V1__CREATE.sql", "V2__ALTER.sql"],
    }

    dag = builder.build(dag_id="test_dag", config=config)

    assert dag.dag_id == "test_dag"
    task_ids = [t.task_id for t in dag.tasks]
    assert task_ids == ["start", "execute_table_schema_changes", "end"]


def test_build_missing_table_ref(builder):
    config = {"scripts": ["V1__CREATE.sql"]}

    with pytest.raises(AirflowException) as exc:
        builder.build(dag_id="test_dag", config=config)

    assert "Missing table_ref" in str(exc.value)


@patch(
    "bigquery_table_management.bigquery_table_management_dag.BigQueryTableManagementDagBuilder.prepare_default_args"
)
def test_build_replaces_env_placeholder(mock_prepare, builder):
    mock_prepare.return_value = {}

    config = {
        "table_ref": "project.dataset_{env}.table",
        "scripts": ["V1__CREATE.sql"],
    }

    dag = builder.build(dag_id="test_dag", config=config)

    # Verify DAG built successfully; {env} replaced with deploy_env (dev)
    assert dag.dag_id == "test_dag"
    execute_task = next(
        t for t in dag.tasks if t.task_id == "execute_table_schema_changes"
    )
    assert execute_task.op_kwargs["table_ref"] == "project.dataset_dev.table"


# ==========================================================
# execute_table_schema_changes
# ==========================================================


@patch("bigquery_table_management.bigquery_table_management_dag.HandlerFactory")
def test_execute_table_schema_changes_success(mock_factory, builder):
    mock_handler = MagicMock()
    mock_handler.execute.side_effect = [True, False]  # First executed, second skipped
    mock_factory.get_handler.return_value = mock_handler

    builder.execute_table_schema_changes(
        table_ref="project.dataset.table",
        table_project_id="project",
        scripts=["V1__CREATE.sql", "V2__ALTER.sql"],
        config={},
    )

    assert mock_factory.get_handler.call_count == 2
    assert mock_handler.execute.call_count == 2


@patch("bigquery_table_management.bigquery_table_management_dag.HandlerFactory")
def test_execute_table_schema_changes_empty_scripts_raises(mock_factory, builder):
    with pytest.raises(AirflowException) as exc:
        builder.execute_table_schema_changes(
            table_ref="project.dataset.table",
            table_project_id="project",
            scripts=[],
            config={},
        )

    assert "No scripts provided" in str(exc.value)
    mock_factory.get_handler.assert_not_called()
