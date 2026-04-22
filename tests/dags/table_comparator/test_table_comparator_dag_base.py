"""
Test cases for TableComparatorBase class.
"""
import pytest
from unittest.mock import patch, MagicMock, mock_open
from datetime import datetime, timedelta
import pendulum
from table_comparator.utils import execute_bigquery_job
from table_comparator.table_comparator_dag_base import TableComparatorBase, INITIAL_DEFAULT_ARGS


@pytest.fixture
def mock_gcp_config():
    """Mock GCP configuration data."""
    return {
        'deployment_environment_name': 'test-env',
        'deploy_env_storage_suffix': 'test-suffix'
    }


@pytest.fixture
def mock_job_config():
    """Mock job configuration data."""
    return {
        'test_dag': {
            'source_table_name': 'test-project.test_dataset.source_table',
            'target_table_name': 'test-project.test_dataset.target_table',
            'primary_key_columns': ['id', 'date'],
            'tags': ['test-tag'],
            'dag': {
                'schedule_interval': '0 1 * * *'
            }
        }
    }


class TestTableComparatorBase:
    """Test cases for TableComparatorBase class."""

    @patch('table_comparator.table_comparator_dag_base.read_variable_or_file')
    @patch('table_comparator.table_comparator_dag_base.read_yamlfile_env_suffix')
    def test_init(
            self,
            mock_read_yaml,
            mock_read_variable,
            mock_gcp_config,
            mock_job_config):
        """Test TableComparatorBase initialization."""
        mock_read_variable.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_job_config

        base = TableComparatorBase('test_config.yaml')

        assert base.gcp_config == mock_gcp_config
        assert base.deploy_env == 'test-env'
        assert base.storage_suffix == 'test-suffix'
        assert base.job_config == mock_job_config
        assert base.default_args == INITIAL_DEFAULT_ARGS.copy()
        assert base.local_tz == pendulum.timezone('America/Toronto')

    def test_get_schedule_with_dag_config(self):
        """Test get_schedule method with DAG configuration."""
        base = TableComparatorBase('test_config.yaml')
        dag_config = {
            'schedule_interval': '0 1 * * *'
        }

        schedule = base.get_schedule(dag_config)
        assert schedule == '0 1 * * *'

    def test_get_schedule_without_dag_config(self):
        """Test get_schedule method without DAG configuration."""
        base = TableComparatorBase('test_config.yaml')
        dag_config = {}

        schedule = base.get_schedule(dag_config)
        assert schedule is None

    @patch('table_comparator.table_comparator_dag_base.read_variable_or_file')
    @patch('table_comparator.table_comparator_dag_base.read_yamlfile_env_suffix')
    def test_generate_dags(
            self,
            mock_read_yaml,
            mock_read_variable,
            mock_gcp_config,
            mock_job_config):
        """Test generate_dags method."""
        mock_read_variable.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_job_config

        base = TableComparatorBase('test_config.yaml')

        with patch.object(base, 'create_dag') as mock_create_dag:
            mock_dag = MagicMock()
            mock_create_dag.return_value = mock_dag

            dags = base.generate_dags()

            assert 'test_dag' in dags
            mock_create_dag.assert_called_once_with(
                'test_dag', mock_job_config['test_dag'])

    def test_initial_default_args(self):
        """Test INITIAL_DEFAULT_ARGS constant."""
        expected_args = {
            "owner": "team-centaurs",
            'capability': 'Terminus Data Platform',
            'severity': 'P3',
            'sub_capability': 'Data Movement',
            'business_impact': 'N/A',
            'customer_impact': 'N/A',
            "email": [],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 10,
            "retry_delay": timedelta(minutes=1),
            "retry_exponential_backoff": True
        }

        assert INITIAL_DEFAULT_ARGS == expected_args

    @patch('table_comparator.table_comparator_dag_base.read_variable_or_file')
    @patch('table_comparator.table_comparator_dag_base.read_yamlfile_env_suffix')
    def test_generate_queries(
            self,
            mock_read_yaml,
            mock_read_variable,
            mock_gcp_config,
            mock_job_config):
        """Test generate_queries method."""
        mock_read_variable.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_job_config

        base = TableComparatorBase('test_config.yaml')
        dag_config = {'source_table_name': 'test.table'}

        result = base.generate_queries(dag_config)

        assert result.task_id == 'generate_comparison_queries_task'
        assert result.python_callable == base.generate_comparison_queries
        assert result.op_kwargs == dag_config

    @patch('table_comparator.table_comparator_dag_base.read_variable_or_file')
    @patch('table_comparator.table_comparator_dag_base.read_yamlfile_env_suffix')
    @patch('table_comparator.table_comparator_dag_base.execute_bigquery_job')
    def test_create_mismatch_report_table(
            self,
            mock_execute,
            mock_read_yaml,
            mock_read_variable,
            mock_gcp_config,
            mock_job_config):
        """Test create_mismatch_report_table method."""
        mock_read_variable.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_job_config

        base = TableComparatorBase('test_config.yaml')
        dag_config = {'mismatch_report_table_name': 'test.report.table'}

        result = base.create_mismatch_report_table(dag_config)

        assert result.task_id == 'create_mismatch_report_table_task'
        assert result.op_kwargs['fetch_results'] is False

    @patch('table_comparator.table_comparator_dag_base.read_variable_or_file')
    @patch('table_comparator.table_comparator_dag_base.read_yamlfile_env_suffix')
    @patch('table_comparator.table_comparator_dag_base.execute_bigquery_job')
    def test_get_schema_mismatches(
            self,
            mock_execute,
            mock_read_yaml,
            mock_read_variable,
            mock_gcp_config,
            mock_job_config):
        """Test get_schema_mismatches method."""
        mock_read_variable.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_job_config

        base = TableComparatorBase('test_config.yaml')
        dag_config = {'source_table_name': 'test.source.table'}

        result = base.get_schema_mismatches(dag_config)

        assert result.task_id == 'get_schema_mismatches_task'
        assert result.op_kwargs['fetch_results'] is True

    @patch('table_comparator.table_comparator_dag_base.read_variable_or_file')
    @patch('table_comparator.table_comparator_dag_base.read_yamlfile_env_suffix')
    @patch('table_comparator.table_comparator_dag_base.execute_bigquery_job')
    def test_get_mismatched_count(
            self,
            mock_execute,
            mock_read_yaml,
            mock_read_variable,
            mock_gcp_config,
            mock_job_config):
        """Test get_mismatched_count method."""
        mock_read_variable.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_job_config

        base = TableComparatorBase('test_config.yaml')
        dag_config = {'source_table_name': 'test.source.table'}

        result = base.get_mismatched_count(dag_config)

        assert result.task_id == 'get_mismatched_count_task'
        assert result.op_kwargs['fetch_results'] is True

    @patch('table_comparator.table_comparator_dag_base.read_variable_or_file')
    @patch('table_comparator.table_comparator_dag_base.read_yamlfile_env_suffix')
    @patch('table_comparator.table_comparator_dag_base.execute_bigquery_job')
    def test_get_missing_in_source_count(
            self,
            mock_execute,
            mock_read_yaml,
            mock_read_variable,
            mock_gcp_config,
            mock_job_config):
        """Test get_missing_in_source_count method."""
        mock_read_variable.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_job_config

        base = TableComparatorBase('test_config.yaml')
        dag_config = {'source_table_name': 'test.source.table'}

        result = base.get_missing_in_source_count(dag_config)

        assert result.task_id == 'get_missing_in_source_count_task'
        assert result.op_kwargs['fetch_results'] is True

    @patch('table_comparator.table_comparator_dag_base.read_variable_or_file')
    @patch('table_comparator.table_comparator_dag_base.read_yamlfile_env_suffix')
    @patch('table_comparator.table_comparator_dag_base.execute_bigquery_job')
    def test_get_missing_in_target_count(
            self,
            mock_execute,
            mock_read_yaml,
            mock_read_variable,
            mock_gcp_config,
            mock_job_config):
        """Test get_missing_in_target_count method."""
        mock_read_variable.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_job_config

        base = TableComparatorBase('test_config.yaml')
        dag_config = {'source_table_name': 'test.source.table'}

        result = base.get_missing_in_target_count(dag_config)

        assert result.task_id == 'get_missing_in_target_count_task'
        assert result.op_kwargs['fetch_results'] is True

    @patch('table_comparator.table_comparator_dag_base.read_variable_or_file')
    @patch('table_comparator.table_comparator_dag_base.read_yamlfile_env_suffix')
    def test_report_summary(
            self,
            mock_read_yaml,
            mock_read_variable,
            mock_gcp_config,
            mock_job_config):
        """Test report_summary method."""
        mock_read_variable.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_job_config

        base = TableComparatorBase('test_config.yaml')
        dag_config = {'source_table_name': 'test.source.table'}

        result = base.report_summary(dag_config)

        assert result.task_id == 'report_summary_task'
        assert result.python_callable == base.report_results
        assert result.op_kwargs == dag_config
        assert result.trigger_rule == 'all_done'

    def test_generate_comparison_queries_abstract(self):
        """Test that generate_comparison_queries is abstract method."""
        base = TableComparatorBase('test_config.yaml')

        # This should be implemented by subclasses
        base.generate_comparison_queries()

    def test_report_results_abstract(self):
        """Test that report_results is abstract method."""
        base = TableComparatorBase('test_config.yaml')

        # This should be implemented by subclasses
        base.report_results()
