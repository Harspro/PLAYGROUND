import pytest
import json
from unittest.mock import patch, MagicMock
from airflow.exceptions import AirflowFailException

from dags.bq_data_patch.generate_data_patch_dags import BigQueryPatchDAG


@pytest.fixture
def mock_gcp_config():
    """Fixture providing mock GCP configuration."""
    return {
        'deployment_environment_name': 'dev'
    }


@pytest.fixture
def sample_data_patch_config():
    """Fixture providing sample data patch configuration."""
    return {
        'test_dag': {
            'dag_owner': 'team-test-alerts',
            'sql_file': 'test_query.sql',
            'tags': ['test']
        }
    }


@pytest.fixture
def sample_sql_content():
    """Fixture providing sample SQL content."""
    return "SELECT * FROM test_table WHERE param1 = '{param1}' AND param2 = '{param2}';"


@pytest.fixture
def dag_builder_fixture(mock_gcp_config, sample_data_patch_config):
    """Fixture providing configured DAG builder instance."""
    with patch('dags.bq_data_patch.generate_data_patch_dags.read_variable_or_file') as mock_read_var, \
         patch('dags.bq_data_patch.generate_data_patch_dags.read_yamlfile_env') as mock_read_yaml, \
         patch('dags.bq_data_patch.generate_data_patch_dags.settings') as mock_settings:

        mock_settings.DAGS_FOLDER = '/test/dags/folder'
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = sample_data_patch_config

        return BigQueryPatchDAG()


class TestExecuteBigQueryPatch:

    @patch('dags.bq_data_patch.generate_data_patch_dags.run_bq_query')
    @patch('dags.bq_data_patch.generate_data_patch_dags.read_file_env')
    def test_execute_bigquery_patch_success_without_parameters(self, mock_read_file, mock_run_query, dag_builder_fixture):
        """Test successful BigQuery patch execution without parameters."""
        sql_content = "SELECT * FROM test_table;"
        mock_read_file.return_value = sql_content
        mock_run_query.return_value = 'job_12345'

        dag_builder_fixture.execute_bigquery_patch('test_query.sql')

        mock_read_file.assert_called_once_with('/test/dags/folder/bq_data_patch/sql/test_query.sql', 'dev')
        mock_run_query.assert_called_once_with(sql_content)

    @patch('dags.bq_data_patch.generate_data_patch_dags.run_bq_query')
    @patch('dags.bq_data_patch.generate_data_patch_dags.read_file_env')
    def test_execute_bigquery_patch_success_with_parameters(self, mock_read_file, mock_run_query, dag_builder_fixture, sample_sql_content):
        """Test successful BigQuery patch execution with parameters."""
        mock_read_file.return_value = sample_sql_content
        mock_run_query.return_value = 'job_67890'

        parameters = {
            'param1': 'value1',
            'param2': 'value2'
        }

        dag_builder_fixture.execute_bigquery_patch('test_query.sql', parameters)

        expected_sql = "SELECT * FROM test_table WHERE param1 = 'value1' AND param2 = 'value2';"
        mock_run_query.assert_called_once_with(expected_sql)

    @patch('dags.bq_data_patch.generate_data_patch_dags.run_bq_query')
    @patch('dags.bq_data_patch.generate_data_patch_dags.read_file_env')
    def test_execute_bigquery_patch_file_not_found(self, mock_read_file, mock_run_query, dag_builder_fixture):
        """Test BigQuery patch execution when SQL file is not found."""
        mock_read_file.side_effect = FileNotFoundError("SQL file not found")

        with pytest.raises(FileNotFoundError, match="SQL file not found"):
            dag_builder_fixture.execute_bigquery_patch('nonexistent.sql')

    @patch('dags.bq_data_patch.generate_data_patch_dags.run_bq_query')
    @patch('dags.bq_data_patch.generate_data_patch_dags.read_file_env')
    def test_execute_bigquery_patch_query_execution_failure(self, mock_read_file, mock_run_query, dag_builder_fixture):
        """Test BigQuery patch execution when query execution fails."""
        sql_content = "SELECT * FROM test_table;"
        mock_read_file.return_value = sql_content
        mock_run_query.side_effect = Exception("BigQuery job failed")

        with pytest.raises(Exception, match="BigQuery job failed"):
            dag_builder_fixture.execute_bigquery_patch('test_query.sql')

    @patch('dags.bq_data_patch.generate_data_patch_dags.run_bq_query')
    @patch('dags.bq_data_patch.generate_data_patch_dags.read_file_env')
    def test_execute_bigquery_patch_missing_parameters(self, mock_read_file, mock_run_query, dag_builder_fixture, sample_sql_content):
        """Test BigQuery patch execution with missing parameters."""
        mock_read_file.return_value = sample_sql_content
        mock_run_query.return_value = 'job_partial'

        parameters = {
            'param1': 'value1'
            # Missing param2
        }

        dag_builder_fixture.execute_bigquery_patch('test_query.sql', parameters)

        # Should execute with unreplaced placeholder
        expected_sql = "SELECT * FROM test_table WHERE param1 = 'value1' AND param2 = '{param2}';"
        mock_run_query.assert_called_once_with(expected_sql)


class TestBuildControlRecordSavingJob:

    @patch('dags.bq_data_patch.generate_data_patch_dags.save_job_to_control_table')
    def test_build_control_record_saving_job_success(self, mock_save_job, dag_builder_fixture):
        """Test successful control record saving."""
        sql_file = 'test_query.sql'
        dag_owner = 'team-test-alerts'

        mock_context = {
            'task_instance': MagicMock(),
            'dag': MagicMock(),
            'dag_run': MagicMock()
        }

        dag_builder_fixture.build_control_record_saving_job(sql_file, dag_owner, **mock_context)

        mock_save_job.assert_called_once()
        call_args = mock_save_job.call_args
        job_params = json.loads(call_args[0][0])

        assert job_params['sql_file'] == sql_file
        assert job_params['dag_owner'] == dag_owner

    @patch('dags.bq_data_patch.generate_data_patch_dags.save_job_to_control_table')
    def test_build_control_record_saving_job_with_none_owner(self, mock_save_job, dag_builder_fixture):
        """Test control record saving with None dag_owner."""
        sql_file = 'test_query.sql'
        dag_owner = None

        mock_context = {
            'task_instance': MagicMock(),
            'dag': MagicMock(),
            'dag_run': MagicMock()
        }

        dag_builder_fixture.build_control_record_saving_job(sql_file, dag_owner, **mock_context)

        mock_save_job.assert_called_once()
        call_args = mock_save_job.call_args
        job_params = json.loads(call_args[0][0])

        assert job_params['sql_file'] == sql_file
        assert job_params['dag_owner'] is None

    @patch('dags.bq_data_patch.generate_data_patch_dags.save_job_to_control_table')
    def test_build_control_record_saving_job_failure(self, mock_save_job, dag_builder_fixture):
        """Test control record saving when save_job_to_control_table fails."""
        sql_file = 'test_query.sql'
        dag_owner = 'team-test-alerts'

        mock_context = {
            'task_instance': MagicMock(),
            'dag': MagicMock(),
            'dag_run': MagicMock()
        }

        mock_save_job.side_effect = Exception("Control table save failed")

        with pytest.raises(Exception, match="Control table save failed"):
            dag_builder_fixture.build_control_record_saving_job(sql_file, dag_owner, **mock_context)

    @patch('dags.bq_data_patch.generate_data_patch_dags.save_job_to_control_table')
    def test_build_control_record_saving_job_json_serialization_error(self, mock_save_job, dag_builder_fixture):
        """Test control record saving when JSON serialization fails."""
        sql_file = 'test_query.sql'
        dag_owner = 'team-test-alerts'

        mock_context = {
            'task_instance': MagicMock(),
            'dag': MagicMock(),
            'dag_run': MagicMock()
        }

        # Mock json.dumps to raise an exception
        with patch('json.dumps') as mock_json_dumps:
            mock_json_dumps.side_effect = TypeError("Object not JSON serializable")

            with pytest.raises(TypeError, match="Object not JSON serializable"):
                dag_builder_fixture.build_control_record_saving_job(sql_file, dag_owner, **mock_context)


class TestReadSQLFile:

    @patch('dags.bq_data_patch.generate_data_patch_dags.read_file_env')
    def test_read_sql_file_success(self, mock_read_file, dag_builder_fixture):
        """Test successful SQL file reading."""
        sql_content = "SELECT * FROM test_table;"
        mock_read_file.return_value = sql_content

        result = dag_builder_fixture.read_sql_file('test_query.sql')

        assert result == sql_content
        mock_read_file.assert_called_once_with('/test/dags/folder/bq_data_patch/sql/test_query.sql', 'dev')

    @patch('dags.bq_data_patch.generate_data_patch_dags.read_file_env')
    def test_read_sql_file_not_found(self, mock_read_file, dag_builder_fixture):
        """Test SQL file reading when file is not found."""
        mock_read_file.side_effect = FileNotFoundError("File not found")

        with pytest.raises(FileNotFoundError, match="File not found"):
            dag_builder_fixture.read_sql_file('nonexistent.sql')

    @patch('dags.bq_data_patch.generate_data_patch_dags.read_file_env')
    def test_read_sql_file_permission_error(self, mock_read_file, dag_builder_fixture):
        """Test SQL file reading when permission is denied."""
        mock_read_file.side_effect = PermissionError("Permission denied")

        with pytest.raises(PermissionError, match="Permission denied"):
            dag_builder_fixture.read_sql_file('restricted.sql')

    @patch('dags.bq_data_patch.generate_data_patch_dags.read_file_env')
    def test_read_sql_file_empty_content(self, mock_read_file, dag_builder_fixture):
        """Test SQL file reading with empty content."""
        mock_read_file.return_value = ""

        result = dag_builder_fixture.read_sql_file('empty.sql')

        assert result == ""
        mock_read_file.assert_called_once_with('/test/dags/folder/bq_data_patch/sql/empty.sql', 'dev')

    @patch('dags.bq_data_patch.generate_data_patch_dags.read_file_env')
    def test_read_sql_file_io_error(self, mock_read_file, dag_builder_fixture):
        """Test SQL file reading when IO error occurs."""
        mock_read_file.side_effect = IOError("IO error occurred")

        with pytest.raises(IOError, match="IO error occurred"):
            dag_builder_fixture.read_sql_file('corrupted.sql')
