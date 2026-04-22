from __future__ import annotations
import os
from unittest.mock import patch, call

import pytest

from airflow import settings
from airflow.exceptions import AirflowFailException

from bigquery_db_connector_loader.bigquery_db_connector_base import BigQueryDbConnector
from util.miscutils import read_file_env, create_airflow_connection

current_script_directory = os.path.dirname(os.path.abspath(__file__))
bigquery_db_connector_config_directory = os.path.join(
    current_script_directory,
    "..",
    "config",
    "bigquery_db_connector_configs"
)


class TestBigQueryDbConnector:
    """
    This test suite covers the functionality of the `BigQueryDbConnector` class.

    The class is responsible for using config files to create DAGs that write data
    to a DB from BigQuery.

    The tests validate various aspects of the classes functionality, including:
    - Ensuring DAGs are created using the correct config parameters provided.
    - Ensuring Operators are called correctly.
    - Error handling when not providing a source query or query file.

    Usage:
    - Run the tests using the pytest command.
    - Utilize coverage package to determine test coverage on operator.
    """

    @pytest.mark.parametrize("config_file, query_file, airflow_connection_details",
                             [
                                 ("test_bq_db_connector_case_1.yaml", False, False),
                                 ("test_bq_db_connector_case_2.yaml", True, True),
                                 ("test_bq_db_connector_case_3.yaml", False, True),
                                 ("test_bq_db_connector_case_4.yaml", True, False),
                                 ("test_bq_db_connector_case_5.yaml", False, False)
                             ])
    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.bigquery")
    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.run_bq_query")
    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.PcfBigQueryToPostgresOperator")
    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.PythonOperator")
    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.pause_unpause_dag")
    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.read_pause_unpause_setting")
    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.read_file_env")
    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.read_variable_or_file")
    def test_bigquery_db_connector_dag_creation(
            self, mock_read_variable_or_file, mock_read_file_env, mock_read_pause_unpause_setting,
            mock_pause_unpause_dag, mock_python_operator, mock_pcf_bigquery_to_postgres_operator,
            mock_run_bq_query, mock_bq, config_file: str, query_file: bool, airflow_connection_details: bool
    ):
        """
        Test BigQuery to DB DAG creation for the `BigQueryDbConnector` class. This is used to
        simulate creating DAGs with various different configuration.

        This test function is parameterized to run with different combinations of
        config files (`config_file`), whether to use a query file (`query_file`) or query,
        and whether to create an Airflow Connection at run time or not.

        :param mock_read_variable_or_file: Fixture providing a mock object for when
            reading variables.
        :type mock_read_variable_or_file: :class:`unittest.mock.MagicMock`

        :param mock_read_file_env: Fixture providing a mock object for when
            reading a file
        :type mock_read_file_env: :class:`unittest.mock.MagicMock`

        :param mock_read_pause_unpause_setting: Fixture providing a mock object
            for determining if a DAG is paused.
        :type mock_read_pause_unpause_setting: :class:`unittest.mock.MagicMock`

        :param mock_pause_unpause_dag: Fixture providing a mock object to
            unpause/pause DAG.
        :type mock_pause_unpause_dag: :class:`unittest.mock.MagicMock`

        :param mock_python_operator: Fixture providing a mock object
            for the python operator.
        :type mock_python_operator: :class:`unittest.mock.MagicMock`

        :param mock_pcf_bigquery_to_postgres_operator: Fixture providing a mock object
            for the PCF BigQuery To Postgres operator.
        :type mock_pcf_bigquery_to_postgres_operator: :class:`unittest.mock.MagicMock`

        :param mock_run_bq_query: Fixture providing a mock object
            for running BigQuery queries.
        :type mock_run_bq_query: :class:`unittest.mock.MagicMock`

        :param config_file: Config file to use.
        :type config_file: str

        :param query_file: Whether a query file will be used.
        :type query_file: bool

        :param airflow_connection_details: Whether to create an Airflow Connection
            at run time or not.
        :type airflow_connection_details: bool
        """
        # Setting the return value for the mocked objects.
        mock_read_variable_or_file.return_value = {
            "deployment_environment_name": "test-deployment-environment"
        }
        mock_read_file_env.return_value = read_file_env(
            f"{current_script_directory}/sql/test_query_file.sql",
            "test-deployment-environment"
        )
        mock_read_pause_unpause_setting.return_value = True
        mock_pause_unpause_dag.return_value = None
        mock_run_bq_query.return_value = (
            BigQueryDbConnector.run_bigquery_staging_query(
                query="test_query",
                project_id="pcb-test-deployment-environment-processing",
                dataset_id="domain_test",
                table_id="test"
            )
        )

        # Asserting parametrized tests.
        if config_file == "test_bq_db_connector_case_5.yaml":
            # Asserting empty configuration files return an empty dictionary.
            assert BigQueryDbConnector(
                config_file,
                bigquery_db_connector_config_directory
            ).create_dags() == {}
        elif config_file == "test_bq_db_connector_case_1.yaml":
            with pytest.raises(AirflowFailException) as e:
                # Initiate DAG creation based on config file.
                globals().update(
                    BigQueryDbConnector(
                        config_file,
                        bigquery_db_connector_config_directory
                    ).create_dags()
                )

            # Check if the correct error message is thrown when not providing a query
            # or query file.
            assert str(e.value) == "Please provide query, none found."
        else:
            # Initiate DAG creation based on config file.
            globals().update(
                BigQueryDbConnector(
                    config_file,
                    bigquery_db_connector_config_directory
                ).create_dags()
            )

            # Verify reading environment variables is done once for gcp config.
            assert mock_read_variable_or_file.call_count == 1

            if query_file:
                # Verify reading the sql file is called once with the correct arguments.
                mock_read_file_env.assert_called_once_with(
                    f"{settings.DAGS_FOLDER}/bigquery_db_connector_loader/sql/test_query_file.sql",
                    "test-deployment-environment"
                )

            # Verifying the python operator is called with the correct arguments.
            expected_python_operator_calls = [
                call(
                    task_id="query_bigquery_table_pre_etl",
                    python_callable=BigQueryDbConnector.run_bigquery_staging_query,
                    op_kwargs={"query": "CREATE OR REPLACE TABLE\n"
                                        "`{staging_table_id}`\n"
                                        "OPTIONS (\n"
                                        "expiration_timestamp = "
                                        "TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)\n"
                                        ") AS (SELECT * FROM "
                                        "`pcb-test-deployment-environment-landing.domain_test.test_table`);",
                               "project_id": "pcb-test-deployment-environment-processing",
                               "dataset_id": "domain_test",
                               "table_id": "test",
                               "source_project_id": None,
                               "source_dataset_id": None,
                               "deploy_env": "test-deployment-environment",
                               "replacements": {},
                               "env_specific_replacements": None
                               }
                )
            ]

            if config_file != "test_bq_db_connector_case_4.yaml":
                # Verifying the PCF BigQuery To Postgres operator is called once with the correct arguments.
                mock_pcf_bigquery_to_postgres_operator.assert_called_once_with(
                    task_id="bigquery_to_postgres",
                    dataset_table="domain_test.test",
                    postgres_conn_id="test_postgres_connection",
                    target_table_name="public.test_target",
                    bigquery_batch_size=10000,
                    sql_batch_size=10000,
                    selected_fields=["test_select_field_1", "test_select_field_2", "test_select_field_3"],
                    update_fields=["test_update_field_1"],
                    conflict_fields=["test_conflict_field_1", "test_conflict_field_2"],
                    append_fields=None,
                    execution_method=None
                )

            if not airflow_connection_details:
                # Verify the read pause/unpause setting is called once with the correct arguments.
                mock_read_pause_unpause_setting.assert_called_once_with(
                    "test_bigquery_db_connector_dag",
                    "test-deployment-environment"
                )
            else:
                # Verify the python operator is called additionally with the correct arguments
                # when creating the Airflow connection.
                expected_python_operator_calls.append(
                    call(
                        task_id="create_airflow_connection",
                        python_callable=create_airflow_connection,
                        op_kwargs={"conn_id": "test_postgres_connection",
                                   "conn_type": "postgres",
                                   "description": "Test Postgres Airflow Connection",
                                   "host": "test-deployment-host",
                                   "login": "test_owner",
                                   "vault_password_secret_path": "/test-secret/data/test-deployment-environment/api",
                                   "vault_password_secret_key": "test.datasource.password",
                                   "schema": "test_schema",
                                   "port": 5432
                                   }
                    )
                )

            mock_python_operator.assert_has_calls(expected_python_operator_calls)

    def test_constructor_with_default_config_dir(self):
        """
        Test BigQueryDbConnector constructor with default config directory.
        """
        with patch(
                "bigquery_db_connector_loader.bigquery_db_connector_base.read_variable_or_file") as mock_read_variable, \
                patch("bigquery_db_connector_loader.bigquery_db_connector_base.read_yamlfile_env") as mock_read_yaml:
            mock_read_variable.return_value = {"deployment_environment_name": "test-env"}
            mock_read_yaml.return_value = {"test_dag": {"bigquery": {"query": "SELECT 1"}}}

            connector = BigQueryDbConnector("test_config.yaml")

            assert connector.deploy_env == "test-env"
            assert connector.config_dir == f"{settings.DAGS_FOLDER}/config/bigquery_db_connector_configs"
            mock_read_variable.assert_called_once()
            mock_read_yaml.assert_called_once()

    def test_constructor_with_custom_config_dir(self):
        """
        Test BigQueryDbConnector constructor with custom config directory.
        """
        with patch(
                "bigquery_db_connector_loader.bigquery_db_connector_base.read_variable_or_file") as mock_read_variable, \
                patch("bigquery_db_connector_loader.bigquery_db_connector_base.read_yamlfile_env") as mock_read_yaml:
            mock_read_variable.return_value = {"deployment_environment_name": "test-env"}
            mock_read_yaml.return_value = {"test_dag": {"bigquery": {"query": "SELECT 1"}}}

            custom_dir = "/custom/config/dir"
            connector = BigQueryDbConnector("test_config.yaml", custom_dir)

            assert connector.config_dir == custom_dir
            mock_read_yaml.assert_called_once_with(f"{custom_dir}/test_config.yaml", "test-env")

    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.bigquery")
    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.run_bq_query")
    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.get_table_columns")
    def test_run_bigquery_staging_query_with_source_table(self, mock_get_columns, mock_run_bq_query, mock_bq):
        """
        Test run_bigquery_staging_query with source project and dataset.
        """
        mock_get_columns.return_value = {"columns": "col1, col2, col3"}

        query = "SELECT {columns} FROM {source_table_id} WHERE {staging_table_id} = 'test'"
        replacements = {"test_key": "test_value"}

        BigQueryDbConnector.run_bigquery_staging_query(
            query=query,
            project_id="test-project",
            dataset_id="test-dataset",
            table_id="test-table",
            source_project_id="source-project",
            source_dataset_id="source-dataset",
            deploy_env="test-env",
            replacements=replacements
        )

        mock_get_columns.assert_called_once_with(mock_bq.Client.return_value,
                                                 "source-project.source-dataset.test-table")
        mock_run_bq_query.assert_called_once()

        # Verify the query was properly formatted
        call_args = mock_run_bq_query.call_args[0][0]
        assert "test-project.test-dataset.test-table" in call_args
        assert "source-project.source-dataset.test-table" in call_args
        assert "col1, col2, col3" in call_args

    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.run_bq_query")
    def test_run_bigquery_staging_query_with_replacements(self, mock_run_bq_query):
        """
        Test run_bigquery_staging_query with environment-specific replacements.
        """
        query = "SELECT {test_key} FROM table WHERE env = '{env_key}'"
        replacements = {
            "common": {"test_key": "common_value"},
            "test-env": {"env_key": "env_value"}
        }

        BigQueryDbConnector.run_bigquery_staging_query(
            query=query,
            project_id="test-project",
            dataset_id="test-dataset",
            table_id="test-table",
            deploy_env="test-env",
            replacements=replacements,
            env_specific_replacements=True
        )

        # Verify the query was properly formatted with replacements
        call_args = mock_run_bq_query.call_args[0][0]
        assert "common_value" in call_args
        assert "env_value" in call_args

    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.run_bq_query")
    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.read_file_env")
    def test_run_bigquery_staging_query_with_query_file(self, mock_read_file, mock_run_bq_query):
        """
        Test run_bigquery_staging_query with query file.
        """
        mock_read_file.return_value = "SELECT * FROM test_table"

        BigQueryDbConnector.run_bigquery_staging_query(
            query_file="test_query.sql",
            project_id="test-project",
            dataset_id="test-dataset",
            table_id="test-table",
            deploy_env="test-env"
        )

        mock_read_file.assert_called_once_with("test_query.sql", "test-env")
        mock_run_bq_query.assert_called_once()

    def test_run_bigquery_staging_query_no_query(self):
        """
        Test run_bigquery_staging_query with no query provided.
        """
        with patch("bigquery_db_connector_loader.bigquery_db_connector_base.logger") as mock_logger:
            BigQueryDbConnector.run_bigquery_staging_query(
                project_id="test-project",
                dataset_id="test-dataset",
                table_id="test-table"
            )

            mock_logger.info.assert_called_with("No query was provided.")

    def test_create_success_callback(self):
        """
        Test create_success_callback method.
        """
        pause_dags = ["dag1", "dag2"]
        unpause_dags = ["dag3", "dag4"]

        callback = BigQueryDbConnector.create_success_callback(pause_dags, unpause_dags)

        assert callable(callback)

        # Test the callback function
        with patch("bigquery_db_connector_loader.bigquery_db_connector_base.pause_unpause_dags") as mock_pause_unpause, \
                patch("bigquery_db_connector_loader.bigquery_db_connector_base.logger"):
            context = {"dag": type("DAG", (), {"dag_id": "test_dag"})()}
            callback(context)

            # Verify unpause was called first
            mock_pause_unpause.assert_any_call(unpause_dags, False)
            # Verify pause was called second
            mock_pause_unpause.assert_any_call(pause_dags, True)

    def test_create_success_callback_empty_lists(self):
        """
        Test create_success_callback with empty lists.
        """
        callback = BigQueryDbConnector.create_success_callback([], [])

        with patch("bigquery_db_connector_loader.bigquery_db_connector_base.pause_unpause_dags") as mock_pause_unpause:
            context = {"dag": type("DAG", (), {"dag_id": "test_dag"})()}
            callback(context)

            # Should not call pause_unpause_dags with empty lists
            mock_pause_unpause.assert_not_called()

    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.read_variable_or_file")
    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.read_yamlfile_env")
    def test_create_dag_with_pause_unpause_at_start(self, mock_read_yaml, mock_read_variable):
        """
        Test create_dag with pause/unpause configuration at start.
        """
        mock_read_variable.return_value = {"deployment_environment_name": "test-env"}
        mock_read_yaml.return_value = {
            "test_dag": {
                "bigquery": {
                    "query": "SELECT 1",
                    "project_id": "test-project",
                    "dataset_id": "test-dataset",
                    "table_id": "test-table"
                },
                "dag": {},
                "dag_pause_unpause_config": {
                    "pause_dags_at_start": ["pause_dag1", "pause_dag2"],
                    "unpause_dags_at_start": ["unpause_dag1"]
                }
            }
        }

        connector = BigQueryDbConnector("test_config.yaml", bigquery_db_connector_config_directory)

        with patch("bigquery_db_connector_loader.bigquery_db_connector_base.PythonOperator") as mock_python_op:
            connector.create_dag("test_dag", connector.job_config["test_dag"])

            # Should create pause and unpause tasks
            assert mock_python_op.call_count >= 2

    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.read_variable_or_file")
    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.read_yamlfile_env")
    def test_create_dag_with_post_etl_query(self, mock_read_yaml, mock_read_variable):
        """
        Test create_dag with post-ETL query configuration.
        """
        mock_read_variable.return_value = {"deployment_environment_name": "test-env"}
        mock_read_yaml.return_value = {
            "test_dag": {
                "bigquery": {
                    "query": "SELECT 1",
                    "project_id": "test-project",
                    "dataset_id": "test-dataset",
                    "table_id": "test-table",
                    "query_file_post_etl": "test_post_etl.sql"
                },
                "postgres": {
                    "postgres_conn_id": "test_conn",
                    "target_table_id": "test_target"
                },
                "airflow_connection_config": {
                    "conn_id": "test_conn",
                    "conn_type": "postgres",
                    "test-env": {
                        "host": "test-host",
                        "login": "test-user",
                        "vault_password_secret_path": "/test/path",
                        "vault_password_secret_key": "test.key",
                        "schema": "test_schema",
                        "port": 5432
                    }
                },
                "dag": {}
            }
        }

        connector = BigQueryDbConnector("test_config.yaml", bigquery_db_connector_config_directory)

        with patch("bigquery_db_connector_loader.bigquery_db_connector_base.read_file_env") as mock_read_file, \
                patch("bigquery_db_connector_loader.bigquery_db_connector_base.PcfBigQueryToPostgresOperator"), \
                patch("bigquery_db_connector_loader.bigquery_db_connector_base.PythonOperator"):
            mock_read_file.return_value = "SELECT * FROM post_etl_table"

            connector.create_dag("test_dag", connector.job_config["test_dag"])

            # Should read the post-ETL query file
            mock_read_file.assert_called()

    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.read_variable_or_file")
    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.read_yamlfile_env")
    def test_create_dag_with_current_date_placeholder(self, mock_read_yaml, mock_read_variable):
        """
        Test create_dag with current date placeholder in table_id.
        """
        mock_read_variable.return_value = {"deployment_environment_name": "test-env"}
        mock_read_yaml.return_value = {
            "test_dag": {
                "bigquery": {
                    "query": "SELECT 1",
                    "project_id": "test-project",
                    "dataset_id": "test-dataset",
                    "table_id": "test_table_{current_date}"
                },
                "dag": {}
            }
        }

        connector = BigQueryDbConnector("test_config.yaml", bigquery_db_connector_config_directory)

        with patch("bigquery_db_connector_loader.bigquery_db_connector_base.PythonOperator") as mock_python_op:
            connector.create_dag("test_dag", connector.job_config["test_dag"])

            # Verify the table_id was processed with current date
            call_kwargs = mock_python_op.call_args[1]["op_kwargs"]
            assert "test_table_" in call_kwargs["table_id"]
            assert call_kwargs["table_id"] != "test_table_{current_date}"

    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.read_variable_or_file")
    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.read_yamlfile_env")
    def test_create_dag_with_dagrun_timeout(self, mock_read_yaml, mock_read_variable):
        """
        Test create_dag with custom dagrun timeout.
        """
        mock_read_variable.return_value = {"deployment_environment_name": "test-env"}
        mock_read_yaml.return_value = {
            "test_dag": {
                "bigquery": {
                    "query": "SELECT 1",
                    "project_id": "test-project",
                    "dataset_id": "test-dataset",
                    "table_id": "test-table"
                },
                "dag": {
                    "dagrun_timeout": 60
                }
            }
        }

        connector = BigQueryDbConnector("test_config.yaml", bigquery_db_connector_config_directory)

        with patch("bigquery_db_connector_loader.bigquery_db_connector_base.PythonOperator"):
            dag = connector.create_dag("test_dag", connector.job_config["test_dag"])

            # Verify dagrun_timeout is set correctly
            assert dag.dagrun_timeout.total_seconds() == 3600  # 60 minutes in seconds

    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.read_variable_or_file")
    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.read_yamlfile_env")
    def test_create_dag_with_tags(self, mock_read_yaml, mock_read_variable):
        """
        Test create_dag with tags configuration.
        """
        mock_read_variable.return_value = {"deployment_environment_name": "test-env"}
        mock_read_yaml.return_value = {
            "test_dag": {
                "bigquery": {
                    "query": "SELECT 1",
                    "project_id": "test-project",
                    "dataset_id": "test-dataset",
                    "table_id": "test-table"
                },
                "dag": {
                    "tags": ["test-tag1", "test-tag2"]
                }
            }
        }

        connector = BigQueryDbConnector("test_config.yaml", bigquery_db_connector_config_directory)

        with patch("bigquery_db_connector_loader.bigquery_db_connector_base.PythonOperator"), \
                patch("bigquery_db_connector_loader.bigquery_db_connector_base.add_tags") as mock_add_tags:
            connector.create_dag("test_dag", connector.job_config["test_dag"])

            # Verify add_tags was called
            mock_add_tags.assert_called_once()

    def test_create_dags_empty_config(self):
        """
        Test create_dags with empty configuration.
        """
        with patch(
                "bigquery_db_connector_loader.bigquery_db_connector_base.read_variable_or_file") as mock_read_variable, \
                patch("bigquery_db_connector_loader.bigquery_db_connector_base.read_yamlfile_env") as mock_read_yaml:
            mock_read_variable.return_value = {"deployment_environment_name": "test-env"}
            mock_read_yaml.return_value = None

            connector = BigQueryDbConnector("test_config.yaml", bigquery_db_connector_config_directory)
            result = connector.create_dags()

            assert result == {}

    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.read_variable_or_file")
    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.read_yamlfile_env")
    def test_create_dags_multiple_dags(self, mock_read_yaml, mock_read_variable):
        """
        Test create_dags with multiple DAG configurations.
        """
        mock_read_variable.return_value = {"deployment_environment_name": "test-env"}
        mock_read_yaml.return_value = {
            "dag1": {
                "bigquery": {
                    "query": "SELECT 1",
                    "project_id": "test-project",
                    "dataset_id": "test-dataset",
                    "table_id": "test-table1"
                },
                "dag": {}
            },
            "dag2": {
                "bigquery": {
                    "query": "SELECT 2",
                    "project_id": "test-project",
                    "dataset_id": "test-dataset",
                    "table_id": "test-table2"
                },
                "dag": {}
            }
        }

        connector = BigQueryDbConnector("test_config.yaml", bigquery_db_connector_config_directory)

        with patch("bigquery_db_connector_loader.bigquery_db_connector_base.PythonOperator"):
            result = connector.create_dags()

            assert len(result) == 2
            assert "dag1" in result
            assert "dag2" in result

    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.read_variable_or_file")
    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.read_yamlfile_env")
    def test_create_dag_with_pause_unpause_at_end(self, mock_read_yaml, mock_read_variable):
        """
        Test create_dag with pause/unpause configuration at end.
        """
        mock_read_variable.return_value = {"deployment_environment_name": "test-env"}
        mock_read_yaml.return_value = {
            "test_dag": {
                "bigquery": {
                    "query": "SELECT 1",
                    "project_id": "test-project",
                    "dataset_id": "test-dataset",
                    "table_id": "test-table"
                },
                "dag": {},
                "dag_pause_unpause_config": {
                    "pause_dags_at_end": ["pause_dag1", "pause_dag2"],
                    "unpause_dags_at_end": ["unpause_dag1"]
                }
            }
        }

        connector = BigQueryDbConnector("test_config.yaml", bigquery_db_connector_config_directory)

        with patch("bigquery_db_connector_loader.bigquery_db_connector_base.PythonOperator"):
            dag = connector.create_dag("test_dag", connector.job_config["test_dag"])

            # Verify DAG has success callback
            assert dag.on_success_callback is not None

    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.read_variable_or_file")
    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.read_yamlfile_env")
    def test_create_dag_with_read_pause_deploy_config(self, mock_read_yaml, mock_read_variable):
        """
        Test create_dag with read_pause_deploy_config enabled.
        """
        mock_read_variable.return_value = {"deployment_environment_name": "test-env"}
        mock_read_yaml.return_value = {
            "test_dag": {
                "bigquery": {
                    "query": "SELECT 1",
                    "project_id": "test-project",
                    "dataset_id": "test-dataset",
                    "table_id": "test-table"
                },
                "dag": {
                    "read_pause_deploy_config": True
                }
            }
        }

        connector = BigQueryDbConnector("test_config.yaml", bigquery_db_connector_config_directory)

        with patch("bigquery_db_connector_loader.bigquery_db_connector_base.PythonOperator"), \
                patch(
                    "bigquery_db_connector_loader.bigquery_db_connector_base.read_pause_unpause_setting") as mock_read_pause, \
                patch(
                    "bigquery_db_connector_loader.bigquery_db_connector_base.pause_unpause_dag") as mock_pause_unpause:
            mock_read_pause.return_value = False

            connector.create_dag("test_dag", connector.job_config["test_dag"])

            # Verify pause/unpause functions were called
            mock_read_pause.assert_called_once_with("test_dag", "test-env")
            mock_pause_unpause.assert_called_once()

    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.read_variable_or_file")
    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.read_yamlfile_env")
    def test_create_dag_with_multiple_start_tasks(self, mock_read_yaml, mock_read_variable):
        """
        Test create_dag with multiple start pause/unpause tasks.
        """
        mock_read_variable.return_value = {"deployment_environment_name": "test-env"}
        mock_read_yaml.return_value = {
            "test_dag": {
                "bigquery": {
                    "query": "SELECT 1",
                    "project_id": "test-project",
                    "dataset_id": "test-dataset",
                    "table_id": "test-table"
                },
                "dag": {},
                "dag_pause_unpause_config": {
                    "pause_dags_at_start": ["pause_dag1"],
                    "unpause_dags_at_start": ["unpause_dag1", "unpause_dag2"]
                }
            }
        }

        connector = BigQueryDbConnector("test_config.yaml", bigquery_db_connector_config_directory)

        with patch("bigquery_db_connector_loader.bigquery_db_connector_base.PythonOperator") as mock_python_op:
            connector.create_dag("test_dag", connector.job_config["test_dag"])

            # Should create multiple start tasks
            assert mock_python_op.call_count >= 3  # pause + 2 unpause tasks

    @patch("bigquery_db_connector_loader.bigquery_db_connector_base.run_bq_query")
    def test_run_bigquery_staging_query_with_target_service_account(self, mock_run_bq_query):
        """
        Test run_bigquery_staging_query with target service account and project.
        """
        query = "SELECT * FROM test_table"

        BigQueryDbConnector.run_bigquery_staging_query(
            query=query,
            project_id="test-project",
            dataset_id="test-dataset",
            table_id="test-table",
            target_service_account="test-service-account",
            target_project="target-project"
        )

        # Verify run_bq_query was called with target parameters
        mock_run_bq_query.assert_called_once()
        call_args = mock_run_bq_query.call_args
        # run_bq_query is called with positional args: query, target_service_account, target_project
        assert call_args[0][0] == query  # First positional arg is the query
        assert call_args[0][1] == "test-service-account"  # Second positional arg is target_service_account
        assert call_args[0][2] == "target-project"  # Third positional arg is target_project
