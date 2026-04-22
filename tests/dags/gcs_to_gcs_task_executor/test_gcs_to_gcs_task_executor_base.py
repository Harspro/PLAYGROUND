"""
Comprehensive pytest tests for GcsToGcsTaskExecutor class.

This module contains tests for the GcsToGcsTaskExecutor class which generates
Airflow DAGs for GCS to GCS file movement/copy operations based on YAML configuration.
"""

import pytest
from datetime import datetime, timedelta
from copy import deepcopy
from unittest.mock import patch, MagicMock, mock_open
import pendulum

# Airflow imports
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

# Internal imports
import sys
import os

import util.constants as consts
from gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base import (
    GcsToGcsTaskExecutor,
    INITIAL_DEFAULT_ARGS
)

# Test configuration
sys.path.append(os.path.join(os.path.dirname(__file__)))
gcs_to_gcs_task_executor_config_directory = os.path.join(
    os.path.dirname(__file__), "..", "config",
    "gcs_to_gcs_task_executor_configs"
)

test_config_file = f"{gcs_to_gcs_task_executor_config_directory}/test_config.yaml"


class TestGcsToGcsTaskExecutor:
    """Test class for GcsToGcsTaskExecutor."""

    @pytest.fixture
    def mock_gcp_config(self):
        """Mock GCP configuration."""
        return {
            consts.DEPLOYMENT_ENVIRONMENT_NAME: 'dev',
            consts.LANDING_ZONE_CONNECTION_ID: 'google_cloud_default',
            consts.DEPLOY_ENV_STORAGE_SUFFIX: '-dev'
        }

    @pytest.fixture
    def mock_job_config(self):
        """Mock job configuration."""
        return {
            'test_dag_1': {
                consts.DEFAULT_ARGS: {
                    'owner': 'test_owner',
                    'depends_on_past': False,
                    'email': ['test@example.com'],
                    'severity': 'P2',
                    'email_on_failure': True,
                    'email_on_retry': False,
                    'retries': 2,
                    'retry_delay': timedelta(minutes=10),
                    'capability': 'test_capability',
                    'sub_capability': 'test_sub_capability',
                    'business_impact': 'high',
                    'customer_impact': 'medium'
                },
                consts.DAG: {
                    'source_bucket': 'source-bucket',
                    'source_object': 'source/path/file.txt',
                    'destination_bucket': 'dest-bucket',
                    'destination_object': 'dest/path/file.txt',
                    'move_object': True,
                    consts.DAGRUN_TIMEOUT: 60,
                    consts.DESCRIPTION: 'Test DAG for GCS to GCS transfer'
                }
            },
            'test_dag_2': {
                consts.DAG: {
                    'source_bucket': 'source-bucket-2',
                    'source_objects': ['file1.txt', 'file2.txt'],
                    'destination_bucket': 'dest-bucket-2',
                    'destination_object': 'dest/path/',
                    'match_glob': '*.txt',
                    'move_object': False,
                    consts.DAGRUN_TIMEOUT: 45
                }
            }
        }

    @pytest.fixture
    def mock_settings(self):
        """Mock Airflow settings."""
        return MagicMock(DAGS_FOLDER='/test/dags')

    def test_initial_default_args_structure(self):
        """Test that INITIAL_DEFAULT_ARGS has correct structure."""
        expected_args = {
            'owner': 'TBD',
            'depends_on_past': False,
            'email': [],
            'severity': 'P3',
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            'capability': 'TBD',
            'sub_capability': 'TBD',
            'business_impact': 'TBD',
            'customer_impact': 'TBD'
        }
        assert INITIAL_DEFAULT_ARGS == expected_args

    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_variable_or_file')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_yamlfile_env_suffix')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.settings')
    def test_init_with_default_config_dir(self, mock_settings, mock_read_yaml, mock_read_var,
                                          mock_gcp_config, mock_job_config):
        """Test initialization with default config directory."""
        mock_settings.DAGS_FOLDER = '/test/dags'
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_job_config

        executor = GcsToGcsTaskExecutor('test_config.yaml')

        assert executor.default_args == deepcopy(INITIAL_DEFAULT_ARGS)
        assert executor.local_tz == pendulum.timezone('America/Toronto')
        assert executor.gcp_config == mock_gcp_config
        assert executor.deploy_env == 'dev'
        assert executor.config_dir == '/test/dags/config/gcs_to_gcs_task_executor_configs'
        assert executor.job_config == mock_job_config

    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_variable_or_file')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_yamlfile_env_suffix')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.settings')
    def test_init_with_custom_config_dir(self, mock_settings, mock_read_yaml, mock_read_var,
                                         mock_gcp_config, mock_job_config):
        """Test initialization with custom config directory."""
        mock_settings.DAGS_FOLDER = '/test/dags'
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_job_config

        custom_config_dir = '/custom/config/dir'
        executor = GcsToGcsTaskExecutor('test_config.yaml', custom_config_dir)

        assert executor.config_dir == custom_config_dir

    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_variable_or_file')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_yamlfile_env_suffix')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.settings')
    def test_create_dag_basic_configuration(self, mock_settings, mock_read_yaml, mock_read_var,
                                            mock_gcp_config, mock_job_config):
        """Test creating a DAG with basic configuration."""
        mock_settings.DAGS_FOLDER = '/test/dags'
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_job_config

        executor = GcsToGcsTaskExecutor('test_config.yaml')
        config = mock_job_config['test_dag_1']
        dag = executor.create_dag('test_dag_1', config)

        # Verify DAG properties
        assert isinstance(dag, DAG)
        assert dag.dag_id == 'test_dag_1'
        assert dag.start_date == datetime(2025, 1, 1, tzinfo=executor.local_tz)
        assert dag.catchup is False
        assert dag.default_args == executor.default_args
        assert dag.dagrun_timeout == timedelta(minutes=60)
        assert dag.description == 'Test DAG for GCS to GCS transfer'
        assert dag.is_paused_upon_creation is True
        assert dag.tags == ['test_owner', 'P2']  # Uses severity and owner from updated default_args

        # Verify tasks exist
        start_task = dag.get_task(consts.START_TASK_ID)
        gcs_task = dag.get_task(f"{consts.GCS_TO_GCS}_task")
        end_task = dag.get_task(consts.END_TASK_ID)

        assert isinstance(start_task, EmptyOperator)
        assert isinstance(gcs_task, GCSToGCSOperator)
        assert isinstance(end_task, EmptyOperator)

        # Verify GCS task configuration
        assert gcs_task.gcp_conn_id == 'google_cloud_default'
        assert gcs_task.source_bucket == 'source-bucket'
        assert gcs_task.source_object == 'source/path/file.txt'
        assert gcs_task.destination_bucket == 'dest-bucket'
        assert gcs_task.destination_object == 'dest/path/file.txt'
        assert gcs_task.move_object is True

    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_variable_or_file')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_yamlfile_env_suffix')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.settings')
    def test_create_dag_with_source_objects(self, mock_settings, mock_read_yaml, mock_read_var,
                                            mock_gcp_config, mock_job_config):
        """Test creating a DAG with source_objects configuration."""
        mock_settings.DAGS_FOLDER = '/test/dags'
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_job_config

        executor = GcsToGcsTaskExecutor('test_config.yaml')
        config = mock_job_config['test_dag_2']
        dag = executor.create_dag('test_dag_2', config)

        gcs_task = dag.get_task(f"{consts.GCS_TO_GCS}_task")
        assert gcs_task.source_objects == ['file1.txt', 'file2.txt']
        assert gcs_task.match_glob == '*.txt'
        assert gcs_task.move_object is False

    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_variable_or_file')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_yamlfile_env_suffix')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.settings')
    def test_create_dag_with_default_timeout(self, mock_settings, mock_read_yaml, mock_read_var,
                                             mock_gcp_config):
        """Test creating a DAG with default timeout when not specified."""
        mock_settings.DAGS_FOLDER = '/test/dags'
        mock_read_var.return_value = mock_gcp_config

        config_without_timeout = {
            consts.DAG: {
                'source_bucket': 'source-bucket',
                'destination_bucket': 'dest-bucket'
            }
        }
        mock_read_yaml.return_value = {'test_dag': config_without_timeout}

        executor = GcsToGcsTaskExecutor('test_config.yaml')
        dag = executor.create_dag('test_dag', config_without_timeout)

        # ✅ Updated: implementation default is now 7 days (10080 minutes)
        assert dag.dagrun_timeout == timedelta(days=7)

    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_variable_or_file')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_yamlfile_env_suffix')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.settings')
    def test_create_dag_task_dependencies(self, mock_settings, mock_read_yaml, mock_read_var,
                                          mock_gcp_config, mock_job_config):
        """Test that DAG tasks have correct dependencies."""
        mock_settings.DAGS_FOLDER = '/test/dags'
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_job_config

        executor = GcsToGcsTaskExecutor('test_config.yaml')
        config = mock_job_config['test_dag_1']
        dag = executor.create_dag('test_dag_1', config)

        start_task = dag.get_task(consts.START_TASK_ID)
        gcs_task = dag.get_task(f"{consts.GCS_TO_GCS}_task")
        end_task = dag.get_task(consts.END_TASK_ID)

        assert gcs_task in start_task.downstream_list
        assert end_task in gcs_task.downstream_list
        assert start_task in gcs_task.upstream_list
        assert gcs_task in end_task.upstream_list

    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_variable_or_file')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_yamlfile_env_suffix')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.settings')
    def test_create_dags_single_config(self, mock_settings, mock_read_yaml, mock_read_var,
                                       mock_gcp_config, mock_job_config):
        """Test create_dags method with single configuration."""
        mock_settings.DAGS_FOLDER = '/test/dags'
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = {'test_dag_1': mock_job_config['test_dag_1']}

        executor = GcsToGcsTaskExecutor('test_config.yaml')
        dags = executor.create_dags()

        assert len(dags) == 1
        assert 'test_dag_1' in dags
        assert isinstance(dags['test_dag_1'], DAG)

    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_variable_or_file')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_yamlfile_env_suffix')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.settings')
    def test_create_dags_multiple_configs(self, mock_settings, mock_read_yaml, mock_read_var,
                                          mock_gcp_config, mock_job_config):
        """Test create_dags method with multiple configurations."""
        mock_settings.DAGS_FOLDER = '/test/dags'
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_job_config

        executor = GcsToGcsTaskExecutor('test_config.yaml')
        dags = executor.create_dags()

        assert len(dags) == 2
        assert 'test_dag_1' in dags
        assert 'test_dag_2' in dags
        assert isinstance(dags['test_dag_1'], DAG)
        assert isinstance(dags['test_dag_2'], DAG)

        dag1 = dags['test_dag_1']
        dag2 = dags['test_dag_2']

        gcs_task1 = dag1.get_task(f"{consts.GCS_TO_GCS}_task")
        gcs_task2 = dag2.get_task(f"{consts.GCS_TO_GCS}_task")

        assert gcs_task1.source_bucket == 'source-bucket'
        assert gcs_task2.source_bucket == 'source-bucket-2'
        assert gcs_task1.move_object is True
        assert gcs_task2.move_object is False

    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_variable_or_file')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_yamlfile_env_suffix')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.settings')
    def test_create_dags_empty_config(self, mock_settings, mock_read_yaml, mock_read_var,
                                      mock_gcp_config):
        """Test create_dags method with empty configuration."""
        mock_settings.DAGS_FOLDER = '/test/dags'
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = {}

        executor = GcsToGcsTaskExecutor('test_config.yaml')
        dags = executor.create_dags()

        assert len(dags) == 0
        assert dags == {}

    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_variable_or_file')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_yamlfile_env_suffix')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.settings')
    def test_create_dags_none_config(self, mock_settings, mock_read_yaml, mock_read_var,
                                     mock_gcp_config):
        """Test create_dags method with None configuration."""
        mock_settings.DAGS_FOLDER = '/test/dags'
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = None

        executor = GcsToGcsTaskExecutor('test_config.yaml')
        dags = executor.create_dags()

        assert len(dags) == 0
        assert dags == {}

    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_variable_or_file')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_yamlfile_env_suffix')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.settings')
    def test_dag_default_args_override(self, mock_settings, mock_read_yaml, mock_read_var,
                                       mock_gcp_config, mock_job_config):
        """Test that DAG default_args override initial default args."""
        mock_settings.DAGS_FOLDER = '/test/dags'
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_job_config

        executor = GcsToGcsTaskExecutor('test_config.yaml')
        config = mock_job_config['test_dag_1']
        dag = executor.create_dag('test_dag_1', config)

        expected_args = config[consts.DEFAULT_ARGS]
        assert dag.default_args == expected_args
        assert dag.default_args['owner'] == 'test_owner'
        assert dag.default_args['severity'] == 'P2'
        assert dag.default_args['retries'] == 2

    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_variable_or_file')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_yamlfile_env_suffix')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.settings')
    def test_dag_default_args_fallback(self, mock_settings, mock_read_yaml, mock_read_var,
                                       mock_gcp_config):
        """Test that DAG falls back to initial default args when not specified."""
        mock_settings.DAGS_FOLDER = '/test/dags'
        mock_read_var.return_value = mock_gcp_config

        config_without_default_args = {
            consts.DAG: {
                'source_bucket': 'source-bucket',
                'destination_bucket': 'dest-bucket'
            }
        }
        mock_read_yaml.return_value = {'test_dag': config_without_default_args}

        executor = GcsToGcsTaskExecutor('test_config.yaml')
        dag = executor.create_dag('test_dag', config_without_default_args)

        assert dag.default_args == INITIAL_DEFAULT_ARGS

    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_variable_or_file')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_yamlfile_env_suffix')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.settings')
    def test_gcs_task_optional_parameters(self, mock_settings, mock_read_yaml, mock_read_var,
                                          mock_gcp_config):
        """Test GCS task with optional parameters."""
        mock_settings.DAGS_FOLDER = '/test/dags'
        mock_read_var.return_value = mock_gcp_config

        config_with_optional_params = {
            consts.DAG: {
                'source_bucket': 'source-bucket',
                'source_object': 'source/path/file.txt',
                'source_objects': ['file1.txt', 'file2.txt'],
                'destination_bucket': 'dest-bucket',
                'destination_object': 'dest/path/file.txt',
                'match_glob': '*.txt',
                'move_object': False
            }
        }
        mock_read_yaml.return_value = {'test_dag': config_with_optional_params}

        executor = GcsToGcsTaskExecutor('test_config.yaml')
        dag = executor.create_dag('test_dag', config_with_optional_params)

        gcs_task = dag.get_task(f"{consts.GCS_TO_GCS}_task")

        assert gcs_task.source_object == 'source/path/file.txt'
        assert gcs_task.source_objects == ['file1.txt', 'file2.txt']
        assert gcs_task.match_glob == '*.txt'
        assert gcs_task.move_object is False

    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_variable_or_file')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_yamlfile_env_suffix')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.settings')
    def test_gcs_task_minimal_parameters(self, mock_settings, mock_read_yaml, mock_read_var,
                                         mock_gcp_config):
        """Test GCS task with minimal required parameters."""
        mock_settings.DAGS_FOLDER = '/test/dags'
        mock_read_var.return_value = mock_gcp_config

        config_minimal = {
            consts.DAG: {
                'source_bucket': 'source-bucket',
                'destination_bucket': 'dest-bucket'
            }
        }
        mock_read_yaml.return_value = {'test_dag': config_minimal}

        executor = GcsToGcsTaskExecutor('test_config.yaml')
        dag = executor.create_dag('test_dag', config_minimal)

        gcs_task = dag.get_task(f"{consts.GCS_TO_GCS}_task")

        assert gcs_task.source_bucket == 'source-bucket'
        assert gcs_task.destination_bucket == 'dest-bucket'
        assert gcs_task.source_object is None
        assert gcs_task.source_objects is None
        assert gcs_task.destination_object is None
        assert gcs_task.match_glob is None
        assert gcs_task.move_object is True

    def test_initial_default_args_immutability(self):
        """Test that INITIAL_DEFAULT_ARGS is not modified during execution."""
        original_args = deepcopy(INITIAL_DEFAULT_ARGS)

        with patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_variable_or_file') as mock_read_var, \
                patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_yamlfile_env_suffix') as mock_read_yaml, \
                patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.settings') as mock_settings:
            mock_settings.DAGS_FOLDER = '/test/dags'
            mock_read_var.return_value = {
                consts.DEPLOYMENT_ENVIRONMENT_NAME: 'dev',
                consts.LANDING_ZONE_CONNECTION_ID: 'google_cloud_default',
                consts.DEPLOY_ENV_STORAGE_SUFFIX: '-dev'
            }
            mock_read_yaml.return_value = {}

            executor = GcsToGcsTaskExecutor('test_config.yaml')
            executor.default_args['owner'] = 'modified_owner'

            assert INITIAL_DEFAULT_ARGS == original_args
            assert INITIAL_DEFAULT_ARGS['owner'] == 'TBD'


class TestGcsToGcsTaskExecutorIntegration:
    """Integration tests for GcsToGcsTaskExecutor."""

    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_variable_or_file')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_yamlfile_env_suffix')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.settings')
    def test_full_workflow_integration(self, mock_settings, mock_read_yaml, mock_read_var):
        """Test complete workflow from initialization to DAG creation."""
        mock_settings.DAGS_FOLDER = '/test/dags'
        mock_read_var.return_value = {
            consts.DEPLOYMENT_ENVIRONMENT_NAME: 'prod',
            consts.LANDING_ZONE_CONNECTION_ID: 'google_cloud_prod',
            consts.DEPLOY_ENV_STORAGE_SUFFIX: '-prod'
        }

        test_config = {
            'production_dag': {
                consts.DEFAULT_ARGS: {
                    'owner': 'data_team',
                    'severity': 'P1',
                    'retries': 3,
                    'retry_delay': timedelta(minutes=15)
                },
                consts.DAG: {
                    'source_bucket': 'prod-source-bucket',
                    'source_object': 'data/input/file.csv',
                    'destination_bucket': 'prod-dest-bucket',
                    'destination_object': 'processed/file.csv',
                    'move_object': True,
                    consts.DAGRUN_TIMEOUT: 120,
                    consts.DESCRIPTION: 'Production data transfer'
                }
            }
        }
        mock_read_yaml.return_value = test_config

        executor = GcsToGcsTaskExecutor('production_config.yaml')
        dags = executor.create_dags()

        assert len(dags) == 1
        assert 'production_dag' in dags

        dag = dags['production_dag']
        assert dag.dag_id == 'production_dag'
        assert dag.default_args['owner'] == 'data_team'
        assert dag.default_args['severity'] == 'P1'
        assert dag.dagrun_timeout == timedelta(minutes=120)
        assert dag.description == 'Production data transfer'
        assert dag.tags == ['data_team', 'P1']

        gcs_task = dag.get_task(f"{consts.GCS_TO_GCS}_task")
        assert gcs_task.gcp_conn_id == 'google_cloud_prod'
        assert gcs_task.source_bucket == 'prod-source-bucket'
        assert gcs_task.destination_bucket == 'prod-dest-bucket'
        assert gcs_task.move_object is True

    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_variable_or_file')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.read_yamlfile_env_suffix')
    @patch('gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base.settings')
    def test_multiple_dags_with_different_configs(self, mock_settings, mock_read_yaml, mock_read_var):
        """Test creating multiple DAGs with different configurations."""
        mock_settings.DAGS_FOLDER = '/test/dags'
        mock_read_var.return_value = {
            consts.DEPLOYMENT_ENVIRONMENT_NAME: 'dev',
            consts.LANDING_ZONE_CONNECTION_ID: 'google_cloud_default',
            consts.DEPLOY_ENV_STORAGE_SUFFIX: '-dev'
        }

        multi_config = {
            'dag_copy': {
                consts.DAG: {
                    'source_bucket': 'source-bucket',
                    'source_object': 'file.txt',
                    'destination_bucket': 'dest-bucket',
                    'destination_object': 'file.txt',
                    'move_object': False
                }
            },
            'dag_move': {
                consts.DAG: {
                    'source_bucket': 'source-bucket',
                    'source_object': 'temp.txt',
                    'destination_bucket': 'dest-bucket',
                    'destination_object': 'temp.txt',
                    'move_object': True
                }
            },
            'dag_batch': {
                consts.DAG: {
                    'source_bucket': 'source-bucket',
                    'source_objects': ['file1.txt', 'file2.txt', 'file3.txt'],
                    'destination_bucket': 'dest-bucket',
                    'destination_object': 'batch/',
                    'match_glob': '*.txt'
                }
            }
        }
        mock_read_yaml.return_value = multi_config

        executor = GcsToGcsTaskExecutor('multi_config.yaml')
        dags = executor.create_dags()

        assert len(dags) == 3

        copy_dag = dags['dag_copy']
        copy_task = copy_dag.get_task(f"{consts.GCS_TO_GCS}_task")
        assert copy_task.move_object is False

        move_dag = dags['dag_move']
        move_task = move_dag.get_task(f"{consts.GCS_TO_GCS}_task")
        assert move_task.move_object is True

        batch_dag = dags['dag_batch']
        batch_task = batch_dag.get_task(f"{consts.GCS_TO_GCS}_task")
        assert batch_task.source_objects == ['file1.txt', 'file2.txt', 'file3.txt']
        assert batch_task.match_glob == '*.txt'
