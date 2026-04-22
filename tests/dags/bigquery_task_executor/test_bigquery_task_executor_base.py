import os
import pytest
from unittest.mock import patch, MagicMock, call, ANY
from datetime import datetime, timedelta

from airflow import DAG, settings
from airflow.exceptions import AirflowFailException

from bigquery_task_executor.bigquery_task_executor_base import BigQueryTaskExecutor
from bigquery_db_connector_loader.bigquery_db_connector_base import BigQueryDbConnector
from util.miscutils import read_file_env

current_script_directory = os.path.dirname(os.path.abspath(__file__))
bigquery_task_executor_config_directory = os.path.join(
    current_script_directory,
    "..",
    "config",
    "bigquery_task_executor_configs"
)


class TestBigQueryTaskExecutor:
    """
    This test suite covers the functionality of the `BigQueryTaskExecutor` class.
    """

    def setup_method(self):
        """Set up any fixtures needed for tests"""
        os.makedirs(bigquery_task_executor_config_directory, exist_ok=True)

    @pytest.mark.parametrize(
        "config_file, config_type",
        [
            ("test_basic_config.yaml", "basic"),
            ("test_query_file_config.yaml", "query_file"),
            ("test_direct_query_config.yaml", "direct_query"),
            ("test_empty_config.yaml", "empty"),
            ("test_missing_query_config.yaml", "missing_query"),
        ],
    )
    @patch("bigquery_task_executor.bigquery_task_executor_base.read_yamlfile_env")
    @patch("bigquery_task_executor.bigquery_task_executor_base.read_variable_or_file")
    @patch("bigquery_task_executor.bigquery_task_executor_base.read_file_env")
    @patch("bigquery_task_executor.bigquery_task_executor_base.PythonOperator")
    @patch("bigquery_task_executor.bigquery_task_executor_base.EmptyOperator")
    @patch("bigquery_task_executor.bigquery_task_executor_base.BigQueryDbConnector.run_bigquery_staging_query")
    def test_bigquery_task_executor_dag_creation(
        self,
        mock_run_bigquery_staging_query,
        mock_empty_operator,
        mock_python_operator,
        mock_read_file_env,
        mock_read_variable_or_file,
        mock_read_yamlfile_env,
        config_file,
        config_type,
    ):
        """Test DAG creation for the BigQueryTaskExecutor class with various configurations."""
        mock_read_variable_or_file.return_value = {
            "deployment_environment_name": "test-deployment-environment"
        }

        mock_read_file_env.return_value = read_file_env(
            f"{current_script_directory}/sql/test_query_file.sql",
            "test-deployment-environment",
        )

        # EmptyOperator mock returns
        start_task_mock = MagicMock()
        end_task_mock = MagicMock()
        mock_empty_operator.side_effect = [start_task_mock, end_task_mock]

        # PythonOperator mock
        task_mock = MagicMock()
        mock_python_operator.return_value = task_mock

        mock_run_bigquery_staging_query.return_value = (
            BigQueryDbConnector.run_bigquery_staging_query(
                query="test_query",
                project_id="pcb-test-deployment-environment-processing",
                dataset_id="domain_test",
                table_id="test",
            )
        )

        # read_file_env mock for SQL query files
        mock_read_file_env.return_value = """
        CREATE OR REPLACE TABLE `{staging_table_id}`
        OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 14 DAY))
        AS SELECT * FROM `pcb-test-deployment-environment-source.dataset.table`
        WHERE run_id = '{run_id}';
        """

        if config_type == "empty":
            mock_read_yamlfile_env.return_value = {}

        elif config_type == "missing_query":
            mock_read_yamlfile_env.return_value = {
                "bigquery_test_dag": {
                    "default_args": {
                        "owner": "test-owner",
                        "capability": "Test Capability",
                        "sub_capability": "Test SubCapability",
                        "severity": "P3",
                        "business_impact": "Test Business Impact",
                        "customer_impact": "Test Customer Impact",
                        "retries": 2,
                        "retry_delay": 300,
                    },
                    "dag": {
                        "dagrun_timeout": 120,
                        "schedule_interval": "@daily",
                        "description": "Test DAG description",
                        "tags": ["BigQueryTest"],
                    },
                    "bigquery_tasks": [
                        {
                            "task_id": "test_task_missing_query",
                            "project_id": "pcb-test-deployment-environment-processing",
                            "dataset_id": "domain_test",
                            "table_id": "TEST_TABLE",
                            "env_specific_replacements": False,
                            "target_service_account": None,
                            "target_project": None,
                        }
                    ],
                }
            }

        elif config_type == "direct_query":
            mock_read_yamlfile_env.return_value = {
                "bigquery_test_dag": {
                    "default_args": {
                        "owner": "test-owner",
                        "capability": "Test Capability",
                        "sub_capability": "Test SubCapability",
                        "severity": "P3",
                        "business_impact": "Test Business Impact",
                        "customer_impact": "Test Customer Impact",
                        "retries": 2,
                        "retry_delay": 300,
                    },
                    "dag": {
                        "dagrun_timeout": 120,
                        "schedule_interval": "@daily",
                        "description": "Test DAG description",
                        "tags": ["BigQueryTest"],
                    },
                    "bigquery_tasks": [
                        {
                            "task_id": "test_direct_query_task",
                            "query": "SELECT * FROM `pcb-test-deployment-environment-processing.domain_test.source_table`",
                            "project_id": "pcb-test-deployment-environment-processing",
                            "dataset_id": "domain_test",
                            "table_id": "TEST_TABLE",
                            "env_specific_replacements": False,
                            "target_service_account": None,
                            "target_project": None,
                        }
                    ],
                }
            }

        elif config_type == "query_file":
            mock_read_yamlfile_env.return_value = {
                "bigquery_test_dag": {
                    "default_args": {
                        "owner": "test-owner",
                        "capability": "Test Capability",
                        "sub_capability": "Test SubCapability",
                        "severity": "P3",
                        "business_impact": "Test Business Impact",
                        "customer_impact": "Test Customer Impact",
                        "retries": 2,
                        "retry_delay": 300,
                    },
                    "dag": {
                        "dagrun_timeout": 120,
                        "schedule_interval": "@daily",
                        "description": "Test DAG description",
                        "tags": ["BigQueryTest"],
                    },
                    "bigquery_tasks": [
                        {
                            "task_id": "test_query_file_task",
                            "query_file": "bigquery_task_executor/query_test_schedule.sql",
                            "project_id": "pcb-test-deployment-environment-processing",
                            "dataset_id": "domain_test",
                            "table_id": "TEST_TABLE",
                            "replacements": {
                                "{run_id}": "{{ run_id }}",
                                "env_specific_replacements": False,
                            },
                            "env_specific_replacements": None,
                            "target_service_account": None,
                            "target_project": None,
                        }
                    ],
                }
            }

        else:  # basic config
            mock_read_yamlfile_env.return_value = {
                "bigquery_test_dag": {
                    "default_args": {
                        "owner": "test-owner",
                        "capability": "Test Capability",
                        "sub_capability": "Test SubCapability",
                        "severity": "P3",
                        "business_impact": "Test Business Impact",
                        "customer_impact": "Test Customer Impact",
                        "retries": 2,
                        "retry_delay": 300,
                    },
                    "dag": {
                        "dagrun_timeout": 120,
                        "schedule_interval": "@daily",
                        "description": "Test DAG description",
                        "tags": ["BigQueryTest"],
                    },
                    "bigquery_tasks": [
                        {
                            "task_id": "test_task_1",
                            "query": "SELECT * FROM `pcb-test-deployment-environment-processing.domain_test.source_table_1`",
                            "project_id": "pcb-test-deployment-environment-processing",
                            "dataset_id": "domain_test",
                            "table_id": "TEST_TABLE_1",
                            "source_project_id": "pcb-test-deployment-environment-source",
                            "source_dataset_id": "domain_source",
                            "deploy_env": "test-deployment-environment",
                        },
                        {
                            "task_id": "test_task_2",
                            "query_file": "bigquery_task_executor/query_test_schedule.sql",
                            "project_id": "pcb-test-deployment-environment-processing",
                            "dataset_id": "domain_test",
                            "table_id": "TEST_TABLE_2_{current_date}",
                            "deploy_env": "test-deployment-environment",
                            "replacements": {
                                "{run_id}": "{{ run_id }}",
                                "env_specific_replacements": False,
                            },
                            "env_specific_replacements": None,
                            "target_service_account": None,
                            "target_project": None,
                        },
                    ],
                }
            }

        executor = BigQueryTaskExecutor(config_file, bigquery_task_executor_config_directory)

        if config_type == "empty":
            assert executor.create_dags() == {}
            return

        if config_type == "missing_query":
            with pytest.raises(AirflowFailException) as exc_info:
                executor.create_dags()
            assert "Please provide query, none found for task." in str(exc_info.value)
            return

        dags = executor.create_dags()
        globals().update(dags)

        assert "bigquery_test_dag" in dags
        assert mock_empty_operator.call_count == 2

        if config_type == "basic":
            assert mock_python_operator.call_count == 2

            expected_calls = [
                call(
                    task_id="test_task_1",
                    python_callable=BigQueryDbConnector.run_bigquery_staging_query,
                    op_kwargs={
                        "query": "SELECT * FROM `pcb-test-deployment-environment-processing.domain_test.source_table_1`",
                        "project_id": "pcb-test-deployment-environment-processing",
                        "dataset_id": "domain_test",
                        "table_id": "TEST_TABLE_1",
                        "source_project_id": "pcb-test-deployment-environment-source",
                        "source_dataset_id": "domain_source",
                        "deploy_env": "test-deployment-environment",
                        "replacements": {},
                        "env_specific_replacements": None,
                        "target_service_account": None,
                        "target_project": None,
                    },
                    execution_timeout=ANY,
                    dag=executor.dag,
                ),
                call(
                    task_id="test_task_2",
                    python_callable=BigQueryDbConnector.run_bigquery_staging_query,
                    op_kwargs={
                        "query": mock_read_file_env.return_value,
                        "project_id": "pcb-test-deployment-environment-processing",
                        "dataset_id": "domain_test",
                        "table_id": ANY,
                        "source_project_id": None,
                        "source_dataset_id": None,
                        "deploy_env": "test-deployment-environment",
                        "replacements": {
                            "{run_id}": "{{ run_id }}",
                            "env_specific_replacements": False,
                        },
                        "env_specific_replacements": None,
                        "target_service_account": None,
                        "target_project": None,
                    },
                    execution_timeout=ANY,
                    dag=executor.dag,
                ),
            ]
            mock_python_operator.assert_has_calls(expected_calls)

            table_id_with_date = mock_python_operator.call_args_list[1][1]["op_kwargs"][
                "table_id"
            ]
            assert "TEST_TABLE_2_" in table_id_with_date
            assert datetime.now(tz=executor.local_tz).strftime("%Y_%m_%d") in table_id_with_date

        elif config_type == "direct_query":
            assert mock_python_operator.call_count == 1

            mock_python_operator.assert_called_once_with(
                task_id="test_direct_query_task",
                python_callable=BigQueryDbConnector.run_bigquery_staging_query,
                op_kwargs={
                    "query": "SELECT * FROM `pcb-test-deployment-environment-processing.domain_test.source_table`",
                    "project_id": "pcb-test-deployment-environment-processing",
                    "dataset_id": "domain_test",
                    "table_id": "TEST_TABLE",
                    "source_project_id": None,
                    "source_dataset_id": None,
                    "deploy_env": "test-deployment-environment",
                    "replacements": {},
                    "env_specific_replacements": False,
                    "target_service_account": None,
                    "target_project": None,
                },
                execution_timeout=ANY,
                dag=executor.dag,
            )

        elif config_type == "query_file":
            assert mock_python_operator.call_count == 1

            mock_read_file_env.assert_called_once_with(
                f"{settings.DAGS_FOLDER}/bigquery_task_executor/query_test_schedule.sql",
                "test-deployment-environment",
            )

            # ✅ include execution_timeout=ANY because the implementation always sets it
            mock_python_operator.assert_called_once_with(
                task_id="test_query_file_task",
                python_callable=BigQueryDbConnector.run_bigquery_staging_query,
                op_kwargs={
                    "query": mock_read_file_env.return_value,
                    "project_id": "pcb-test-deployment-environment-processing",
                    "dataset_id": "domain_test",
                    "table_id": "TEST_TABLE",
                    "source_project_id": None,
                    "source_dataset_id": None,
                    "deploy_env": "test-deployment-environment",
                    "replacements": {
                        "{run_id}": "{{ run_id }}",
                        "env_specific_replacements": False,
                    },
                    "env_specific_replacements": None,
                    "target_service_account": None,
                    "target_project": None,
                },
                execution_timeout=ANY,
                dag=executor.dag,
            )

        dag = dags["bigquery_test_dag"]
        assert dag.dag_id == "bigquery_test_dag"
        assert dag.description == "Test DAG description"
        assert dag.tags == ["BigQueryTest"]
        assert dag.dagrun_timeout == timedelta(minutes=120)

        assert dag.default_args["owner"] == "test-owner"
        assert dag.default_args["capability"] == "Test Capability"
        assert dag.default_args["severity"] == "P3"

        if config_type in ["basic", "direct_query", "query_file"]:
            assert mock_empty_operator.call_args_list[0][1]["task_id"] == "start"
            assert mock_empty_operator.call_args_list[1][1]["task_id"] == "end"

    @patch("bigquery_task_executor.bigquery_task_executor_base.read_variable_or_file")
    @patch("bigquery_task_executor.bigquery_task_executor_base.read_yamlfile_env")
    def test_create_dags_with_multiple_configs(self, mock_read_yaml, mock_read_variable):
        mock_read_variable.return_value = {"deployment_environment_name": "test-deployment-environment"}
        mock_read_yaml.return_value = {
            "dag_1": {"dag": {"schedule": "@daily"}, "bigquery_tasks": []},
            "dag_2": {"dag": {"schedule": "@hourly"}, "bigquery_tasks": []},
        }
        executor = BigQueryTaskExecutor("test_config.yaml")
        dags = executor.create_dags()
        assert len(dags) == 2

    @patch("bigquery_task_executor.bigquery_task_executor_base.read_variable_or_file")
    @patch("util.miscutils.read_file_env", side_effect=FileNotFoundError("File not found"))
    def test_create_bigquery_tasks_with_missing_file(self, mock_read_file, mock_read_variable):
        mock_read_variable.return_value = {"deployment_environment_name": "test-deployment-environment"}
        config = {
            # ✅ add dag section to avoid KeyError('dag') in implementation
            "dag": {"dagrun_timeout": 120},
            "bigquery_tasks": [
                {
                    "task_id": "test_task",
                    "query_file": "missing_file.sql",
                    "project_id": "test-project",
                    "dataset_id": "test_dataset",
                }
            ],
        }
        executor = BigQueryTaskExecutor("test_config.yaml")
        executor.dag = DAG(dag_id="dummy_dag", schedule=None, start_date=datetime(2023, 1, 1))
        with pytest.raises(AirflowFailException):
            executor._create_bigquery_tasks(config)

    @patch("bigquery_task_executor.bigquery_task_executor_base.read_variable_or_file")
    def test_init_with_explicit_config_dir(self, mock_read_variable):
        mock_read_variable.return_value = {"deployment_environment_name": "test-deployment-environment"}
        config_dir = f"{settings.DAGS_FOLDER}/config/bigquery_task_executor_configs"
        executor = BigQueryTaskExecutor("bigquery_task_executor_config.yaml", config_dir)
        assert executor.config_dir == config_dir

    @patch("bigquery_task_executor.bigquery_task_executor_base.PythonOperator")
    @patch("bigquery_task_executor.bigquery_task_executor_base.EmptyOperator")
    @patch("bigquery_task_executor.bigquery_task_executor_base.read_variable_or_file")
    def test_create_bigquery_tasks_with_date_placeholder(
        self, mock_read_variable, mock_empty_operator, mock_python_operator
    ):
        """Test task creation with CURRENT_DATE_PLACEHOLDER in table_id"""
        mock_read_variable.return_value = {"deployment_environment_name": "test-deployment-environment"}
        config = {
            # ✅ add dag section to avoid KeyError('dag') in implementation
            "dag": {"dagrun_timeout": 120},
            "bigquery_tasks": [
                {
                    "task_id": "test_task",
                    "query": "SELECT 1",
                    "project_id": "test-project",
                    "dataset_id": "test_dataset",
                    "table_id": "table_{current_date}",
                }
            ],
        }
        executor = BigQueryTaskExecutor("test_config.yaml")
        executor.dag = DAG(dag_id="dummy_dag", schedule=None, start_date=datetime(2023, 1, 1))
        executor._create_bigquery_tasks(config)

        assert mock_empty_operator.call_count == 2
        assert mock_empty_operator.call_args_list[0][1]["task_id"] == "start"
        assert mock_empty_operator.call_args_list[1][1]["task_id"] == "end"

        mock_python_operator.assert_called_once()
        call_args = mock_python_operator.call_args[1]
        table_id = call_args["op_kwargs"]["table_id"]
        expected_date = datetime.now(tz=executor.local_tz).strftime("%Y_%m_%d")
        assert table_id == f"table_{expected_date}"

    @patch("bigquery_task_executor.bigquery_task_executor_base.read_variable_or_file")
    def test_create_dags_with_empty_config(self, mock_read_variable):
        mock_read_variable.return_value = {"deployment_environment_name": "test-deployment-environment"}
        executor = BigQueryTaskExecutor("test_config.yaml")
        executor.job_config = {}
        dags = executor.create_dags()
        assert dags == {}

    @patch("bigquery_task_executor.bigquery_task_executor_base.read_variable_or_file")
    @patch("bigquery_task_executor.bigquery_task_executor_base.read_yamlfile_env")
    def test_create_dags_with_invalid_yaml(self, mock_read_yamlfile_env, mock_read_variable):
        mock_read_variable.return_value = {"deployment_environment_name": "test-deployment-environment"}
        mock_read_yamlfile_env.side_effect = AirflowFailException("Invalid YAML in configuration file")
        with pytest.raises(AirflowFailException):
            BigQueryTaskExecutor("invalid_config.yaml")
