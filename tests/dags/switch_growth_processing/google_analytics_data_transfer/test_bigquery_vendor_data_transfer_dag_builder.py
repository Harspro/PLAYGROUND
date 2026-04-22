"""
Tests for GoogleAnalyticsVendorDataTransfer DAG builder (create_table_copying_tasks, build).
Positive and negative scenarios per method.
"""
import copy
import pytest
from datetime import timedelta
from unittest.mock import patch, MagicMock

import dags.util.constants as consts
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule
from dags.switch_growth_processing.google_analytics_data_transfer.google_analytics_vendor_data_transfer import (
    GoogleAnalyticsVendorDataTransfer,
    DAG_DEFAULT_ARGS,
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
    return GoogleAnalyticsVendorDataTransfer(environment_config_fixture)


@pytest.fixture
def sample_bigquery_config():
    """Fixture providing minimal valid BigQuery config. Returns a copy so tests can mutate safely."""
    return copy.deepcopy({
        'audit_table_ref': 'proj.ds.audit',
        'service_account': 'svc@pcb-{env}-processing.iam.gserviceaccount.com',
        'table_name': 'events',
        'source': {
            'dev': {'project_id': 'src-proj', 'dataset_id': 'src_ds'},
            'uat': {'project_id': 'src-proj', 'dataset_id': 'src_ds'},
            'prod': {'project_id': 'src-proj', 'dataset_id': 'src_ds'},
        },
        'landing': {'project_id': 'land-proj', 'dataset_id': 'land_ds'},
        'vendor': {
            'dev': {'project_id': 'vend-dev', 'dataset_id': 'vend_ds'},
            'uat': {'project_id': 'vend-uat', 'dataset_id': 'vend_ds'},
            'prod': {'project_id': 'vend-prod', 'dataset_id': 'vend_ds'},
        },
    })


@pytest.fixture
def sample_dag_config(sample_bigquery_config):
    """Fixture providing minimal DAG config for daily load (dag_config with bigquery inside)."""
    return {
        'load_type': 'daily',
        consts.BIGQUERY: sample_bigquery_config,
    }


class TestDAGDefaultArgs:
    """Test DAG default arguments configuration."""

    def test_retries_set_to_zero(self):
        """Test that retries is set to 0 to prevent unnecessary re-replication."""
        assert DAG_DEFAULT_ARGS['retries'] == 0

    def test_retry_delay_configured(self):
        """Test that retry_delay is configured (even though retries=0)."""
        assert DAG_DEFAULT_ARGS['retry_delay'] == timedelta(minutes=3)

    def test_email_on_retry_disabled(self):
        """Test that email_on_retry is False."""
        assert DAG_DEFAULT_ARGS['email_on_retry'] is False

    def test_execution_timeout_configured(self):
        """Test that execution_timeout is set."""
        assert DAG_DEFAULT_ARGS['execution_timeout'] == timedelta(minutes=30)

    def test_depends_on_past_disabled(self):
        """Test that depends_on_past is False."""
        assert DAG_DEFAULT_ARGS['depends_on_past'] is False


class TestBigQueryVendorDataTransferInit:

    def test_init_sets_deploy_env(self):
        """Test that __init__ sets deploy_env from environment_config."""
        env = MagicMock()
        env.deploy_env = 'uat'
        builder = GoogleAnalyticsVendorDataTransfer(env)
        assert builder.deploy_env == 'uat'


class TestCreateTableCopyingTasks:

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.google_analytics_vendor_data_transfer.build_iteration_items')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.google_analytics_vendor_data_transfer.extract_and_validate_config')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.google_analytics_vendor_data_transfer.create_single_table_task_group')
    def test_create_table_copying_tasks_success(
        self, mock_create_single_table_task_group, mock_extract_config,
        mock_build_iteration, dag_builder_fixture, sample_dag_config
    ):
        """Test successful creation of task groups (daily path)."""
        mock_extract_config.return_value = {
            'audit_table_ref': 'proj.ds.audit',
            'service_account': 'svc@proj.iam.gserviceaccount.com',
            'source_project': 'src-proj',
            'source_dataset': 'src_ds',
            'landing_project': 'land-proj',
            'landing_dataset': 'land_ds',
            'vendor_project': 'vend-dev',
            'vendor_dataset': 'vend_ds',
            'write_disposition': 'WRITE_TRUNCATE',
            'create_disposition': 'CREATE_IF_NEEDED',
        }
        mock_build_iteration.return_value = [
            {'task_group_id': 'events', 'table_name': 'events', 'date': None},
        ]
        mock_task_group = MagicMock()
        mock_task_group.group_id = 'events'
        mock_create_single_table_task_group.return_value = mock_task_group

        task_groups = dag_builder_fixture.create_table_copying_tasks(sample_dag_config)

        mock_extract_config.assert_called_once_with(sample_dag_config, 'dev')
        mock_build_iteration.assert_called_once_with(sample_dag_config)
        mock_create_single_table_task_group.assert_called_once()
        assert len(task_groups) == 1
        assert task_groups[0].group_id == 'events'

    def test_create_table_copying_tasks_missing_bigquery_raises(
        self, dag_builder_fixture
    ):
        """Test missing bigquery in dag_config raises AirflowException."""
        dag_config = {'load_type': 'daily'}

        with pytest.raises(AirflowException, match='Missing.*bigquery.*configuration'):
            dag_builder_fixture.create_table_copying_tasks(dag_config)

    def test_create_table_copying_tasks_missing_audit_table_ref_raises(
        self, dag_builder_fixture, sample_dag_config
    ):
        """Test missing audit_table_ref raises AirflowException."""
        del sample_dag_config[consts.BIGQUERY]['audit_table_ref']

        with pytest.raises(AirflowException, match='Missing.*audit_table_ref'):
            dag_builder_fixture.create_table_copying_tasks(sample_dag_config)

    def test_create_table_copying_tasks_missing_service_account_raises(
        self, dag_builder_fixture, sample_dag_config
    ):
        """Test missing service_account raises AirflowException."""
        del sample_dag_config[consts.BIGQUERY]['service_account']

        with pytest.raises(AirflowException, match='Missing.*service_account'):
            dag_builder_fixture.create_table_copying_tasks(sample_dag_config)

    def test_create_table_copying_tasks_missing_source_raises(
        self, dag_builder_fixture, sample_dag_config
    ):
        """Test missing source config raises AirflowException."""
        del sample_dag_config[consts.BIGQUERY]['source']

        with pytest.raises(AirflowException, match='Missing.*source'):
            dag_builder_fixture.create_table_copying_tasks(sample_dag_config)

    def test_create_table_copying_tasks_missing_landing_raises(
        self, dag_builder_fixture, sample_dag_config
    ):
        """Test missing landing config raises AirflowException."""
        del sample_dag_config[consts.BIGQUERY]['landing']

        with pytest.raises(AirflowException, match='Missing.*landing'):
            dag_builder_fixture.create_table_copying_tasks(sample_dag_config)

    def test_create_table_copying_tasks_missing_vendor_raises(
        self, dag_builder_fixture, sample_dag_config
    ):
        """Test missing vendor config raises AirflowException."""
        del sample_dag_config[consts.BIGQUERY]['vendor']

        with pytest.raises(AirflowException, match='Missing.*vendor'):
            dag_builder_fixture.create_table_copying_tasks(sample_dag_config)

    def test_create_table_copying_tasks_missing_vendor_for_deploy_env_raises(
        self, dag_builder_fixture, sample_dag_config
    ):
        """Test vendor without entry for deploy_env raises AirflowException."""
        sample_dag_config[consts.BIGQUERY]['vendor'] = {'uat': {'project_id': 'p', 'dataset_id': 'd'}}

        with pytest.raises(AirflowException, match="Missing.*vendor.*environment.*dev"):
            dag_builder_fixture.create_table_copying_tasks(sample_dag_config)

    def test_create_table_copying_tasks_missing_source_for_deploy_env_raises(
        self, dag_builder_fixture, sample_dag_config
    ):
        """Test source without entry for deploy_env raises AirflowException."""
        sample_dag_config[consts.BIGQUERY]['source'] = {'uat': {'project_id': 'p', 'dataset_id': 'd'}}

        with pytest.raises(AirflowException, match="Missing.*source.*environment.*dev"):
            dag_builder_fixture.create_table_copying_tasks(sample_dag_config)


class TestBuild:

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.google_analytics_vendor_data_transfer.GoogleAnalyticsVendorDataTransfer.create_table_copying_tasks')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.google_analytics_vendor_data_transfer.EmptyOperator')
    def test_build_success(
        self, mock_empty, mock_create_tasks, dag_builder_fixture,
        sample_bigquery_config, sample_dag_config
    ):
        """Test successful DAG build with start/end tasks and table copying tasks."""
        mock_create_tasks.return_value = []
        config = {
            consts.BIGQUERY: sample_bigquery_config,
            consts.DEFAULT_ARGS: {},
        }

        dag = dag_builder_fixture.build('test_dag_id', config)

        assert dag.dag_id == 'test_dag_id'
        assert dag.max_active_runs == 1
        assert dag.catchup is False
        assert dag.is_paused_upon_creation is True
        assert dag.max_active_tasks == 5
        assert dag.dagrun_timeout == timedelta(minutes=180)
        assert mock_empty.call_count == 2
        mock_empty.assert_any_call(task_id=consts.START_TASK_ID)
        mock_empty.assert_any_call(task_id=consts.END_TASK_ID, trigger_rule=TriggerRule.ALL_SUCCESS)
        mock_create_tasks.assert_called_once_with(config)

    def test_build_missing_bigquery_config_raises(self, dag_builder_fixture):
        """Test missing bigquery config raises AirflowException."""
        config = {consts.DEFAULT_ARGS: {}}

        with pytest.raises(AirflowException, match='Missing.*bigquery.*configuration'):
            dag_builder_fixture.build('test_dag_id', config)

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.google_analytics_vendor_data_transfer.GoogleAnalyticsVendorDataTransfer.create_table_copying_tasks')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.google_analytics_vendor_data_transfer.EmptyOperator')
    def test_build_merges_default_args(
        self, mock_empty, mock_create_tasks, dag_builder_fixture, sample_bigquery_config
    ):
        """Test that build merges config default_args into DAG default_args."""
        mock_create_tasks.return_value = []
        config = {
            consts.BIGQUERY: sample_bigquery_config,
            consts.DEFAULT_ARGS: {'owner': 'data-engineering', 'retries': 2},
        }

        dag = dag_builder_fixture.build('custom_dag', config)

        assert dag.default_args.get('owner') == 'data-engineering'
        assert dag.default_args.get('retries') == 2

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.google_analytics_vendor_data_transfer.GoogleAnalyticsVendorDataTransfer.create_table_copying_tasks')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.google_analytics_vendor_data_transfer.EmptyOperator')
    def test_build_uses_default_retries_zero(
        self, mock_empty, mock_create_tasks, dag_builder_fixture, sample_bigquery_config
    ):
        """Test that build uses default retries=0 when not overridden in config."""
        mock_create_tasks.return_value = []
        config = {
            consts.BIGQUERY: sample_bigquery_config,
            consts.DEFAULT_ARGS: {'owner': 'data-engineering'},
        }

        dag = dag_builder_fixture.build('test_dag', config)

        assert dag.default_args.get('retries') == 0

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.google_analytics_vendor_data_transfer.GoogleAnalyticsVendorDataTransfer.create_table_copying_tasks')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.google_analytics_vendor_data_transfer.EmptyOperator')
    def test_build_sets_schedule_from_config_daily_multi_schedule(
        self, mock_empty, mock_create_tasks, dag_builder_fixture, sample_bigquery_config
    ):
        """Test that build uses schedule_interval from config (e.g. daily multi-schedule 08:30, 11:30, 14:30, 16:30)."""
        mock_create_tasks.return_value = []
        daily_multi_schedule = "30 8,11,14,16 * * *"
        config = {
            consts.BIGQUERY: sample_bigquery_config,
            consts.DEFAULT_ARGS: {},
            'schedule_interval': daily_multi_schedule,
            'load_type': 'daily',
        }

        dag = dag_builder_fixture.build('test_dag_id', config)

        assert dag.dag_id == 'test_dag_id'
        # DAG build uses config['schedule_interval'] as schedule=; DAG exposes it as schedule_interval or schedule
        schedule_value = getattr(dag, 'schedule_interval', None) or getattr(dag, 'schedule', None)
        assert schedule_value == daily_multi_schedule
