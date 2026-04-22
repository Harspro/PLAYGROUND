import json
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from google.cloud import bigquery
from airflow.exceptions import AirflowFailException

from dags.operations.bigquery_bulk_data_cleanup import (
    BigQueryBulkDataCleanupBuilder
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
    return BigQueryBulkDataCleanupBuilder(environment_config_fixture)


@pytest.fixture
def mock_bigquery_client():
    """Fixture providing mock BigQuery client."""
    mock_client = MagicMock()

    mock_count_result = MagicMock()
    mock_count_result.get.return_value = 5

    mock_delete_result = MagicMock()

    def mock_query_side_effect(query):
        mock_query_job = MagicMock()
        if 'COUNT(*)' in query:
            mock_query_job.result.return_value = iter([mock_count_result])
        else:
            mock_query_job.result.return_value = iter([mock_delete_result])
        return mock_query_job

    mock_client.query.side_effect = mock_query_side_effect
    return mock_client


@pytest.fixture
def sample_config():
    """Fixture providing sample DAG configuration."""
    return {
        'filter': "FILE_CREATE_DT = '2025-01-01'",
        'bigquery': {
            'project_id': 'test-project',
            'dataset_id': 'test_dataset',
            'table_id': ['table1', 'table2']
        },
        'tags': ['data-cleanup', 'bigquery']
    }


class TestCheckTableAndCreateBackup:

    @patch('dags.operations.bigquery_bulk_data_cleanup.build_back_up_job')
    @patch('dags.operations.bigquery_bulk_data_cleanup.check_bq_table_exists')
    def test_check_table_and_create_backup_success(self, mock_check_table, mock_build_backup, dag_builder_fixture):
        """Test successful table check and backup creation."""
        table_ref = 'test-project.test_dataset.test_table'
        mock_check_table.return_value = True
        mock_build_backup.return_value = None

        result = dag_builder_fixture.check_table_and_create_backup(table_ref)

        assert result is True
        mock_check_table.assert_called_once_with(table_ref)
        mock_build_backup.assert_called_once_with(table_ref, backup_enabled=True)

    @patch('dags.operations.bigquery_bulk_data_cleanup.check_bq_table_exists')
    def test_check_table_and_create_backup_table_not_exists(self, mock_check_table, dag_builder_fixture):
        """Test failure when table does not exist."""
        table_ref = 'test-project.test_dataset.nonexistent_table'
        mock_check_table.return_value = False

        with pytest.raises(AirflowFailException, match=f'Table {table_ref} does not exist'):
            dag_builder_fixture.check_table_and_create_backup(table_ref)

        mock_check_table.assert_called_once_with(table_ref)


class TestBigQueryBulkDataCleanupBuilder:

    @patch('dags.operations.bigquery_bulk_data_cleanup.bigquery.Client')
    def test_remove_records_success(self, mock_client_class, dag_builder_fixture, mock_bigquery_client):
        """Test successful record removal."""
        table_ref = 'test-project.test_dataset.test_table'
        filter_condition = "FILE_CREATE_DT = '2025-01-01'"

        mock_client_class.return_value = mock_bigquery_client

        dag_builder_fixture.remove_records(table_ref, filter_condition)

        assert mock_bigquery_client.query.call_count == 2

        count_call = mock_bigquery_client.query.call_args_list[0]
        assert f'SELECT COUNT(*) AS REC_COUNT FROM `{table_ref}` WHERE {filter_condition};' in count_call[0][0]

        delete_call = mock_bigquery_client.query.call_args_list[1]
        assert f'DELETE FROM `{table_ref}` WHERE {filter_condition};' in delete_call[0][0]

    @patch('dags.operations.bigquery_bulk_data_cleanup.bigquery.Client')
    def test_remove_records_zero_records(self, mock_client_class, dag_builder_fixture):
        """Test record removal when no records match filter."""
        table_ref = 'test-project.test_dataset.test_table'
        filter_condition = "FILE_CREATE_DT = '2025-01-01'"

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_count_result = MagicMock()
        mock_count_result.get.return_value = 0
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = iter([mock_count_result])
        mock_client.query.return_value = mock_query_job

        dag_builder_fixture.remove_records(table_ref, filter_condition)

        assert mock_client.query.call_count == 2

    @patch('dags.operations.bigquery_bulk_data_cleanup.save_job_to_control_table')
    def test_build_control_record_saving_job_success(self, mock_save_job, dag_builder_fixture):
        """Test build_control_record_saving_job method."""
        filter_condition = "FILE_CREATE_DT = '2025-01-01'"
        mock_context = {
            'dag': MagicMock(dag_id='test_dag'),
            'dag_run': MagicMock(run_id='test_run_123')
        }

        dag_builder_fixture.build_control_record_saving_job(filter_condition, **mock_context)

        mock_save_job.assert_called_once()
        call_args = mock_save_job.call_args
        job_params_str = call_args[0][0]
        job_params = json.loads(job_params_str)

        assert 'operation' in job_params
        assert 'filter' in job_params
        assert job_params['filter'] == filter_condition

    @patch('dags.operations.bigquery_bulk_data_cleanup.save_job_to_control_table')
    def test_build_control_record_saving_job_no_dag_run(self, mock_save_job, dag_builder_fixture):
        """Test control record saving when dag_run is not in context."""
        filter_condition = "FILE_CREATE_DT = '2025-01-01'"
        mock_context = {
            'dag': MagicMock(dag_id='test_dag')
            # No 'dag_run' key
        }

        dag_builder_fixture.build_control_record_saving_job(filter_condition, **mock_context)

        mock_save_job.assert_called_once()
        call_args = mock_save_job.call_args
        job_params_str = call_args[0][0]
        job_params = json.loads(job_params_str)

        assert 'operation' in job_params
        assert 'filter' in job_params
        assert job_params['filter'] == filter_condition

    @patch('dags.operations.bigquery_bulk_data_cleanup.DAG')
    @patch('dags.operations.bigquery_bulk_data_cleanup.EmptyOperator')
    @patch('dags.operations.bigquery_bulk_data_cleanup.PythonOperator')
    @patch('dags.operations.bigquery_bulk_data_cleanup.TaskGroup')
    def test_build_dag_success(self, mock_task_group, mock_python_operator, mock_empty_operator, mock_dag_class, dag_builder_fixture, sample_config):
        """Test successful DAG building."""
        dag_id = 'test_bulk_cleanup_dag'
        config = sample_config

        mock_dag = MagicMock()
        mock_dag_class.return_value.__enter__.return_value = mock_dag

        result = dag_builder_fixture.build(dag_id, config)

        assert result == mock_dag
        mock_dag_class.assert_called_once()
        mock_empty_operator.assert_called()

    def test_build_dag_missing_required_fields(self, dag_builder_fixture):
        """Test DAG building with missing required fields."""
        dag_id = 'test_dag'
        config = {
            'filter': "FILE_CREATE_DT = '2025-01-01'",
            'bigquery': {
                'project_id': 'test_project'
            }
        }

        with pytest.raises(AirflowFailException, match=f'Missing required fields in config for {dag_id}'):
            dag_builder_fixture.build(dag_id, config)

    def test_build_dag_missing_filter(self, dag_builder_fixture):
        """Test DAG building with missing filter."""
        dag_id = 'test_dag'
        config = {
            'bigquery': {
                'project_id': 'test_project',
                'dataset_id': 'test_dataset',
                'table_id': ['table1']
            }
        }

        with pytest.raises(AirflowFailException, match=f'Filter Condition is required in config for {dag_id}'):
            dag_builder_fixture.build(dag_id, config)

    def test_build_dag_empty_table_ids(self, dag_builder_fixture):
        """Test DAG building with empty table_ids list."""
        dag_id = 'test_dag'
        config = {
            'filter': "FILE_CREATE_DT = '2025-01-01'",
            'bigquery': {
                'project_id': 'test_project',
                'dataset_id': 'test_dataset',
                'table_id': []
            }
        }

        with pytest.raises(AirflowFailException, match=f'Missing required fields in config for {dag_id}'):
            dag_builder_fixture.build(dag_id, config)

    @patch('dags.operations.bigquery_bulk_data_cleanup.DAG')
    @patch('dags.operations.bigquery_bulk_data_cleanup.EmptyOperator')
    @patch('dags.operations.bigquery_bulk_data_cleanup.PythonOperator')
    @patch('dags.operations.bigquery_bulk_data_cleanup.TaskGroup')
    def test_build_dag_with_custom_args(self, mock_task_group, mock_python_operator, mock_empty_operator, mock_dag_class, dag_builder_fixture):
        """Test DAG building with custom default_args."""
        dag_id = 'test_dag'
        config = {
            'filter': "FILE_CREATE_DT = '2025-01-01'",
            'bigquery': {
                'project_id': 'test_project',
                'dataset_id': 'test_dataset',
                'table_id': ['table1']
            },
            'default_args': {
                'owner': 'test_owner',
                'retries': 2
            }
        }

        mock_dag = MagicMock()
        mock_dag_class.return_value.__enter__.return_value = mock_dag

        result = dag_builder_fixture.build(dag_id, config)

        assert result == mock_dag
        call_args = mock_dag_class.call_args
        assert call_args[1]['default_args']['owner'] == 'test_owner'
        assert call_args[1]['default_args']['retries'] == 2

    @patch('dags.operations.bigquery_bulk_data_cleanup.DAG')
    @patch('dags.operations.bigquery_bulk_data_cleanup.EmptyOperator')
    @patch('dags.operations.bigquery_bulk_data_cleanup.PythonOperator')
    @patch('dags.operations.bigquery_bulk_data_cleanup.TaskGroup')
    def test_build_dag_with_tags(self, mock_task_group, mock_python_operator, mock_empty_operator, mock_dag_class, dag_builder_fixture):
        """Test DAG building with tags."""
        dag_id = 'test_dag'
        config = {
            'filter': "FILE_CREATE_DT = '2025-01-01'",
            'bigquery': {
                'project_id': 'test_project',
                'dataset_id': 'test_dataset',
                'table_id': ['table1']
            },
            'tags': ['test', 'cleanup', 'bigquery']
        }

        mock_dag = MagicMock()
        mock_dag_class.return_value.__enter__.return_value = mock_dag

        result = dag_builder_fixture.build(dag_id, config)

        assert result == mock_dag
        call_args = mock_dag_class.call_args
        assert call_args[1]['tags'] == ['test', 'cleanup', 'bigquery']

    @patch('dags.operations.bigquery_bulk_data_cleanup.DAG')
    @patch('dags.operations.bigquery_bulk_data_cleanup.EmptyOperator')
    @patch('dags.operations.bigquery_bulk_data_cleanup.PythonOperator')
    @patch('dags.operations.bigquery_bulk_data_cleanup.TaskGroup')
    def test_build_dag_task_group_creation(self, mock_task_group, mock_python_operator, mock_empty_operator, mock_dag_class, dag_builder_fixture):
        """Test that TaskGroups are created for each table."""
        dag_id = 'test_dag'
        config = {
            'filter': "FILE_CREATE_DT = '2025-01-01'",
            'bigquery': {
                'project_id': 'test_project',
                'dataset_id': 'test_dataset',
                'table_id': ['TABLE_1', 'TABLE_2', 'TABLE_3']
            }
        }

        mock_dag = MagicMock()
        mock_dag_class.return_value.__enter__.return_value = mock_dag

        dag_builder_fixture.build(dag_id, config)

        assert mock_task_group.call_count == 3

        task_group_calls = mock_task_group.call_args_list
        group_ids = [call[1]['group_id'] for call in task_group_calls]
        expected_group_ids = ['process_table_1', 'process_table_2', 'process_table_3']
        assert group_ids == expected_group_ids

    @patch('dags.operations.bigquery_bulk_data_cleanup.DAG')
    @patch('dags.operations.bigquery_bulk_data_cleanup.EmptyOperator')
    @patch('dags.operations.bigquery_bulk_data_cleanup.PythonOperator')
    @patch('dags.operations.bigquery_bulk_data_cleanup.TaskGroup')
    def test_build_dag_python_operators_creation(self, mock_task_group, mock_python_operator, mock_empty_operator, mock_dag_class, dag_builder_fixture):
        """Test that PythonOperators are created correctly."""
        dag_id = 'test_dag'
        config = {
            'filter': "FILE_CREATE_DT = '2025-01-01'",
            'bigquery': {
                'project_id': 'test_project',
                'dataset_id': 'test_dataset',
                'table_id': ['table1']
            }
        }

        mock_dag = MagicMock()
        mock_dag_class.return_value.__enter__.return_value = mock_dag

        dag_builder_fixture.build(dag_id, config)

        assert mock_python_operator.call_count >= 2

        python_operator_calls = mock_python_operator.call_args_list
        task_ids = [call[1]['task_id'] for call in python_operator_calls]
        assert 'create_backup_table' in task_ids
        assert 'data_cleanup' in task_ids
        assert 'save_job_to_control_table' in task_ids


class TestIntegration:
    """Integration tests for the complete workflow."""

    @patch('dags.operations.bigquery_bulk_data_cleanup.DAG')
    @patch('dags.operations.bigquery_bulk_data_cleanup.EmptyOperator')
    @patch('dags.operations.bigquery_bulk_data_cleanup.PythonOperator')
    @patch('dags.operations.bigquery_bulk_data_cleanup.TaskGroup')
    def test_complete_dag_structure(self, mock_task_group, mock_python_operator,
                                    mock_empty_operator, mock_dag_class,
                                    dag_builder_fixture):
        """Test the complete DAG structure and task dependencies."""
        dag_id = 'integration_test_dag'
        config = {
            'filter': "FILE_CREATE_DT = '2025-01-01'",
            'bigquery': {
                'project_id': 'test_project',
                'dataset_id': 'test_dataset',
                'table_id': ['table1', 'table2']
            }
        }

        mock_dag = MagicMock()
        mock_dag_class.return_value.__enter__.return_value = mock_dag

        result = dag_builder_fixture.build(dag_id, config)

        assert result == mock_dag

        dag_call_args = mock_dag_class.call_args
        assert dag_call_args[1]['dag_id'] == dag_id
        assert dag_call_args[1]['schedule'] is None
        assert dag_call_args[1]['catchup'] is False
        assert dag_call_args[1]['max_active_runs'] == 1
        assert dag_call_args[1]['is_paused_upon_creation'] is True
        assert dag_call_args[1]['max_active_tasks'] == 10

        assert mock_task_group.call_count == 2

        assert mock_python_operator.call_count >= 4

        assert mock_empty_operator.call_count == 2
