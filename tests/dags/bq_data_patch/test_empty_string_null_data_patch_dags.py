import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from google.cloud import bigquery
from airflow.exceptions import AirflowFailException

from dags.bq_data_patch.empty_string_null_data_patch_dags import (
    EmptyStringToNullPatcher,
    get_string_columns,
    build_update_query
)


@pytest.fixture
def environment_config_fixture():
    """Fixture providing mock EnvironmentConfig."""
    environment_config = MagicMock()
    environment_config.deploy_env = 'dev'
    return environment_config


@pytest.fixture
def dag_builder_fixture(environment_config_fixture):
    """Fixture providing configured DAG builder instance."""
    return EmptyStringToNullPatcher(environment_config_fixture)


@pytest.fixture
def mock_bigquery_table():
    """Fixture providing mock BigQuery table with string columns."""
    mock_table = MagicMock()

    # Create simple objects that behave like BigQuery schema fields
    class MockField:
        def __init__(self, field_type, name):
            self.field_type = field_type
            self.name = name

    mock_table.schema = [
        MockField('STRING', 'name'),
        MockField('STRING', 'email'),
        MockField('STRING', 'description')
    ]
    return mock_table


@pytest.fixture
def sample_config():
    """Fixture providing sample DAG configuration."""
    return {
        'test_dag_id': {
            'table_ref': 'gcp-project.bq_dataset.test_table',
            'dag_schedule': None,
            'tags': ['data-patch', 'bigquery']
        }
    }


class TestEmptyStringToNullPatcher:

    @patch('dags.bq_data_patch.empty_string_null_data_patch_dags.create_backup_table')
    def test_create_table_backup(self, mock_create_backup, dag_builder_fixture):
        table_ref = 'gcp-project.bq_dataset.test_table'

        with patch('dags.bq_data_patch.empty_string_null_data_patch_dags.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 0, 0)

            backup_table_ref = dag_builder_fixture.create_table_backup(table_ref)

            expected_backup_table_ref = 'gcp-project.bq_dataset.test_table_backup_20250101_120000'
            assert backup_table_ref == expected_backup_table_ref

            mock_create_backup.assert_called_once_with(expected_backup_table_ref, table_ref)

    @patch('dags.bq_data_patch.empty_string_null_data_patch_dags.run_bq_query')
    @patch('dags.bq_data_patch.empty_string_null_data_patch_dags.get_string_columns')
    @patch('dags.bq_data_patch.empty_string_null_data_patch_dags.build_update_query')
    def test_execute_update_query_success(self, mock_build_query, mock_get_columns, mock_run_query, dag_builder_fixture):
        table_ref = 'gcp-project.bq_dataset.test_table'

        mock_get_columns.return_value = ['name', 'email', 'description']
        mock_build_query.return_value = "UPDATE `gcp-project.bq_dataset.test_table` SET name = NULLIF(TRIM(name), ''), email = NULLIF(TRIM(email), ''), description = NULLIF(TRIM(description), '') WHERE 1=1;"
        mock_run_query.return_value = 'job_123'

        result = dag_builder_fixture.execute_update_query(table_ref)

        assert result == "Update completed for gcp-project.bq_dataset.test_table"

        mock_get_columns.assert_called_once_with(table_ref)
        mock_build_query.assert_called_once_with(table_ref, ['name', 'email', 'description'])
        mock_run_query.assert_called_once_with("UPDATE `gcp-project.bq_dataset.test_table` SET name = NULLIF(TRIM(name), ''), email = NULLIF(TRIM(email), ''), description = NULLIF(TRIM(description), '') WHERE 1=1;")

    @patch('dags.bq_data_patch.empty_string_null_data_patch_dags.get_string_columns')
    def test_execute_update_query_no_string_columns(self, mock_get_columns, dag_builder_fixture):
        table_ref = 'gcp-project.bq_dataset.test_table'
        mock_get_columns.return_value = []

        with pytest.raises(AirflowFailException, match="No string columns found for table gcp-project.bq_dataset.test_table"):
            dag_builder_fixture.execute_update_query(table_ref)

    @patch('dags.bq_data_patch.empty_string_null_data_patch_dags.save_job_to_control_table')
    def test_build_control_record_saving_job(self, mock_save_job, dag_builder_fixture):
        """Test build_control_record_saving_job method."""
        table_ref = 'gcp-project.bq_dataset.test_table'
        backup_table_ref = 'gcp-project.bq_dataset.test_table_backup_20250101_120000'

        mock_context = {
            'task_instance': MagicMock(),
            'dag': MagicMock(),
            'dag_run': MagicMock()
        }
        mock_context['task_instance'].xcom_pull.return_value = backup_table_ref

        dag_builder_fixture.build_control_record_saving_job(table_ref, **mock_context)

        mock_context['task_instance'].xcom_pull.assert_called_once_with(task_ids='create_table_backup')

        mock_save_job.assert_called_once()
        call_args = mock_save_job.call_args
        assert 'source_table_ref' in call_args[0][0]
        assert 'backup_table_ref' in call_args[0][0]


class TestUtilityFunctions:

    @patch('dags.bq_data_patch.empty_string_null_data_patch_dags.bigquery.Client')
    def test_get_string_columns(self, mock_bigquery_client, mock_bigquery_table):
        # Configure the mock to return our mock client instance
        mock_client_instance = MagicMock()
        mock_client_instance.get_table.return_value = mock_bigquery_table
        mock_bigquery_client.return_value = mock_client_instance

        # Call the function - it should use our mocked client
        string_columns = get_string_columns('gcp-project.bq_dataset.test_table')

        expected_columns = ['name', 'email', 'description']
        assert string_columns == expected_columns

        # Verify the mock was called correctly
        mock_bigquery_client.assert_called_once()
        mock_client_instance.get_table.assert_called_once_with('gcp-project.bq_dataset.test_table')

    def test_build_update_query_success(self):
        table_ref = 'gcp-project.bq_dataset.test_table'
        string_columns = ['name', 'email', 'description']

        update_query = build_update_query(table_ref, string_columns)

        assert 'UPDATE `gcp-project.bq_dataset.test_table`' in update_query
        assert 'SET' in update_query
        assert 'name = NULLIF(TRIM(name), \'\')' in update_query
        assert 'email = NULLIF(TRIM(email), \'\')' in update_query
        assert 'description = NULLIF(TRIM(description), \'\')' in update_query
        assert 'WHERE 1=1;' in update_query

    def test_build_update_query_no_string_columns(self):
        table_ref = 'gcp-project.bq_dataset.test_table'
        string_columns = []

        with pytest.raises(AirflowFailException, match="No string columns found for table gcp-project.bq_dataset.test_table"):
            build_update_query(table_ref, string_columns)

    def test_build_update_query_single_column(self):
        table_ref = 'gcp-project.bq_dataset.test_table'
        string_columns = ['name']

        update_query = build_update_query(table_ref, string_columns)

        assert 'UPDATE `gcp-project.bq_dataset.test_table`' in update_query
        assert 'SET' in update_query
        assert 'name = NULLIF(TRIM(name), \'\')' in update_query
        assert 'WHERE 1=1;' in update_query
