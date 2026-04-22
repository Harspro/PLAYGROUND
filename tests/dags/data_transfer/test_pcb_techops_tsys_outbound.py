"""Unit tests for PCBTechopsTsysOutboundFileTransfer DAG."""

import pytest
from unittest.mock import patch, call, MagicMock
from dataclasses import asdict
from datetime import timedelta
from airflow.exceptions import AirflowFailException
from util.auditing_utils.model.audit_record import AuditRecord
from data_transfer.pcb_techops_tsys_outbound import PCBTechopsTsysOutboundFileTransfer


@pytest.fixture
def dag_context_fixture():
    """Fixture providing mock DAG context."""
    mock_dag = MagicMock()
    mock_dag.dag_id = "pcb_techops_tsys_outbound"

    mock_dag_run = MagicMock()
    mock_dag_run.conf = {
        "source_file_path": "gs://pcb-dev-staging-extract/pcb_tsys_pcmc_monsbm/pcb_tsys_pcmc_monsbm_inc6789_20250724100445.uatv"
    }

    mock_task_instance = MagicMock()
    mock_task_instance.xcom_push.return_value = None
    mock_task_instance.xcom_pull.return_value = None

    return {
        'dag_run': mock_dag_run,
        'dag': mock_dag,
        'ti': mock_task_instance,
        'run_id': 'manual__2025-02-11T16:15:10+00:00'
    }


@pytest.fixture
def audit_record_fixture():
    """Fixture providing expected audit record."""
    expected_audit_record_values = """
                        'pcb-dev-processing',
                        '',
                        '',
                        'file',
                        'gs://pcb-dev-staging-extract/pcb_tsys_pcmc_monsbm/pcb_tsys_pcmc_monsbm_inc6789_20250724100445.uatv',
                        'pcb_tsys_pcmc_monsbm_inc6789_20250724100445.uatv',
                        'pcb-dev-landing',
                        'gs://tsys-outbound-pcb-dev/pcb_tsys_pcmc_monsbm/pcb_tsys_pcmc_monsbm_inc6789_20250724100445.uatv',
                        'pcb_techops_tsys_outbound',
                        'manual__2025-02-11T16:15:10+00:00',
                        'Vendor_Data_Outbound',
                        'manual',
                        'pcb-dev-processing',
                        CURRENT_TIMESTAMP()
          """

    audit_record = AuditRecord(
        dataset_id='domain_technical',
        table_id='DATA_TRANSFER_AUDIT',
        audit_record_values=expected_audit_record_values
    )

    return audit_record


@pytest.fixture
def dag_builder_fixture():
    """Fixture providing configured DAG builder instance."""
    # Create a mock or minimal EnvironmentConfig
    environment_config = MagicMock()
    environment_config.gcp_config = {
        'processing_zone_project_id': 'pcb-dev-processing',
        'landing_zone_project_id': 'pcb-dev-landing',
        'deploy_env_storage_suffix': '-dev'
    }
    # Use a proper timezone object instead of MagicMock
    from pendulum import timezone
    environment_config.local_tz = timezone('America/Toronto')
    environment_config.storage_suffix = '-dev'

    builder = PCBTechopsTsysOutboundFileTransfer(environment_config)

    # Set up dag_config for tests that need it
    builder.dag_config = {
        'pcb_techops_tsys_outbound': {
            'destination_bucket_name': 'tsys-outbound-pcb'
        }
    }

    return builder


class TestPCBTechopsTsysOutboundFileTransfer:
    """Test class for PCBTechopsTsysOutboundFileTransfer."""

    @patch('data_transfer.pcb_techops_tsys_outbound.create_data_transfer_audit_table_if_not_exists')
    @patch('data_transfer.pcb_techops_tsys_outbound.AuditUtil')
    @patch('google.cloud.bigquery.Client')
    def test_prepare_config_success(self, mock_bigquery_client, mock_audit_util, mock_create_table, dag_builder_fixture, dag_context_fixture, audit_record_fixture):
        """Test successful prepare_config execution."""
        builder = dag_builder_fixture

        # Mock BigQuery client
        mock_client_instance = MagicMock()
        mock_query = MagicMock()
        mock_query.result.return_value = None
        mock_client_instance.query.return_value = mock_query
        mock_bigquery_client.return_value = mock_client_instance

        # Mock audit util
        mock_audit_instance = MagicMock()
        mock_audit_util.return_value = mock_audit_instance
        builder.audit_util = mock_audit_instance

        # Mock xcom push
        mock_ti = dag_context_fixture['ti']

        # Call the method
        builder.prepare_config(**dag_context_fixture)

        # Verify xcom pushes
        expected_calls = [
            call(key='audit_record', value=asdict(audit_record_fixture)),
            call(key='source_bucket_name', value='pcb-dev-staging-extract'),
            call(key='source_file_path', value='pcb_tsys_pcmc_monsbm/pcb_tsys_pcmc_monsbm_inc6789_20250724100445.uatv'),
            call(key='source_file_name', value='pcb_tsys_pcmc_monsbm_inc6789_20250724100445.uatv'),
            call(key='destination_bucket', value='tsys-outbound-pcb-dev'),
            call(key='destination_file_path', value='pcb_tsys_pcmc_monsbm/pcb_tsys_pcmc_monsbm_inc6789_20250724100445.uatv')
        ]
        mock_ti.xcom_push.assert_has_calls(expected_calls, any_order=True)

        # Verify audit record was created
        builder.audit_util.record_request_received.assert_called_once()

    @patch('data_transfer.pcb_techops_tsys_outbound.create_data_transfer_audit_table_if_not_exists')
    @patch('data_transfer.pcb_techops_tsys_outbound.AuditUtil')
    @patch('google.cloud.bigquery.Client')
    def test_prepare_config_invalid_source_path(self, mock_bigquery_client, mock_audit_util, mock_create_table, dag_builder_fixture, dag_context_fixture):
        """Test prepare_config with invalid source path."""
        builder = dag_builder_fixture

        # Mock BigQuery client
        mock_client_instance = MagicMock()
        mock_query = MagicMock()
        mock_query.result.return_value = None
        mock_client_instance.query.return_value = mock_query
        mock_bigquery_client.return_value = mock_client_instance

        # Mock audit util to prevent any real calls
        mock_audit_instance = MagicMock()
        mock_audit_util.return_value = mock_audit_instance
        builder.audit_util = mock_audit_instance

        # Modify context to have invalid source path
        dag_context_fixture['dag_run'].conf = {
            "source_file_path": "invalid_path_without_gs_prefix"
        }

        # Call the method and expect AirflowFailException
        with pytest.raises(AirflowFailException, match="source_file_path must start with 'gs://'"):
            builder.prepare_config(**dag_context_fixture)

    @patch('data_transfer.pcb_techops_tsys_outbound.gcs_file_exists')
    def test_validate_source_file_already_transferred_in_path(self, mock_gcs_file_exists, dag_builder_fixture, dag_context_fixture):
        """Test validate_source_file with file path containing .transferred."""
        builder = dag_builder_fixture

        # Mock xcom pull returns with .transferred in path
        mock_ti = dag_context_fixture['ti']
        mock_ti.xcom_pull.side_effect = [
            'pcb-dev-staging-extract',  # source_bucket_name
            'pcb_tsys_pcmc_monsbm/pcb_tsys_pcmc_monsbm_inc6789_20250724100445.uatv.transferred',  # source_file_path with .transferred
            'pcb_tsys_pcmc_monsbm_inc6789_20250724100445.uatv.transferred'  # source_file_name with .transferred
        ]

        # Call the method and expect AirflowFailException
        with pytest.raises(AirflowFailException, match="Cannot transfer file.*as it already has .transferred suffix"):
            builder.validate_source_file(**dag_context_fixture)

    @patch('data_transfer.pcb_techops_tsys_outbound.gcs_file_exists')
    def test_validate_source_file_already_transferred_in_filename(self, mock_gcs_file_exists, dag_builder_fixture, dag_context_fixture):
        """Test validate_source_file with filename containing .transferred."""
        builder = dag_builder_fixture

        # Mock xcom pull returns with .transferred in filename only
        mock_ti = dag_context_fixture['ti']
        mock_ti.xcom_pull.side_effect = [
            'pcb-dev-staging-extract',  # source_bucket_name
            'pcb_tsys_pcmc_monsbm/pcb_tsys_pcmc_monsbm_inc6789_20250724100445.uatv',  # source_file_path without .transferred
            'pcb_tsys_pcmc_monsbm_inc6789_20250724100445.uatv.transferred'  # source_file_name with .transferred
        ]

        # Call the method and expect AirflowFailException
        with pytest.raises(AirflowFailException, match="Cannot transfer file.*as it already has .transferred suffix"):
            builder.validate_source_file(**dag_context_fixture)

    @patch('data_transfer.pcb_techops_tsys_outbound.create_data_transfer_audit_table_if_not_exists')
    @patch('data_transfer.pcb_techops_tsys_outbound.AuditUtil')
    @patch('google.cloud.bigquery.Client')
    def test_prepare_config_missing_dag_config(self, mock_bigquery_client, mock_audit_util, mock_create_table, dag_builder_fixture, dag_context_fixture):
        """Test prepare_config with missing DAG configuration."""
        builder = dag_builder_fixture

        # Mock BigQuery client
        mock_client_instance = MagicMock()
        mock_query = MagicMock()
        mock_query.result.return_value = None
        mock_client_instance.query.return_value = mock_query
        mock_bigquery_client.return_value = mock_client_instance

        # Mock audit util to prevent any real calls
        mock_audit_instance = MagicMock()
        mock_audit_util.return_value = mock_audit_instance
        builder.audit_util = mock_audit_instance

        builder.dag_config = {}  # Empty config

        # Call the method and expect AirflowFailException
        with pytest.raises(AirflowFailException, match="Configuration not found for DAG ID"):
            builder.prepare_config(**dag_context_fixture)

    @patch('data_transfer.pcb_techops_tsys_outbound.gcs_file_exists')
    def test_validate_source_file_success(self, mock_gcs_file_exists, dag_builder_fixture, dag_context_fixture):
        """Test successful validate_source_file execution."""
        builder = dag_builder_fixture

        # Mock xcom pull returns
        mock_ti = dag_context_fixture['ti']
        mock_ti.xcom_pull.side_effect = [
            'pcb-dev-staging-extract',  # source_bucket_name
            'pcb_tsys_pcmc_monsbm/pcb_tsys_pcmc_monsbm_inc6789_20250724100445.uatv',  # source_file_path
            'pcb_tsys_pcmc_monsbm_inc6789_20250724100445.uatv'  # source_file_name
        ]

        # Mock gcs_file_exists to return False for .transferred file and True for original file
        mock_gcs_file_exists.side_effect = [False, True]

        # Call the method
        builder.validate_source_file(**dag_context_fixture)

        # Verify gcs_file_exists was called correctly
        expected_calls = [
            call('pcb-dev-staging-extract', 'pcb_tsys_pcmc_monsbm/pcb_tsys_pcmc_monsbm_inc6789_20250724100445.uatv.transferred'),
            call('pcb-dev-staging-extract', 'pcb_tsys_pcmc_monsbm/pcb_tsys_pcmc_monsbm_inc6789_20250724100445.uatv')
        ]
        mock_gcs_file_exists.assert_has_calls(expected_calls)

    @patch('data_transfer.pcb_techops_tsys_outbound.gcs_file_exists')
    def test_validate_source_file_transferred_version_exists(self, mock_gcs_file_exists, dag_builder_fixture, dag_context_fixture):
        """Test validate_source_file when .transferred version already exists."""
        builder = dag_builder_fixture

        # Mock xcom pull returns
        mock_ti = dag_context_fixture['ti']
        mock_ti.xcom_pull.side_effect = [
            'pcb-dev-staging-extract',  # source_bucket_name
            'pcb_tsys_pcmc_monsbm/pcb_tsys_pcmc_monsbm_inc6789_20250724100445.uatv',  # source_file_path
            'pcb_tsys_pcmc_monsbm_inc6789_20250724100445.uatv'  # source_file_name
        ]

        # Mock gcs_file_exists to return True for .transferred file
        mock_gcs_file_exists.return_value = True

        # Call the method and expect ValueError
        with pytest.raises(AirflowFailException, match="A .transferred version of this file already exists"):
            builder.validate_source_file(**dag_context_fixture)

    @patch('data_transfer.pcb_techops_tsys_outbound.gcs_file_exists')
    def test_validate_source_file_original_file_not_found(self, mock_gcs_file_exists, dag_builder_fixture, dag_context_fixture):
        """Test validate_source_file when original file doesn't exist."""
        builder = dag_builder_fixture

        # Mock xcom pull returns
        mock_ti = dag_context_fixture['ti']
        mock_ti.xcom_pull.side_effect = [
            'pcb-dev-staging-extract',  # source_bucket_name
            'pcb_tsys_pcmc_monsbm/pcb_tsys_pcmc_monsbm_inc6789_20250724100445.uatv',  # source_file_path
            'pcb_tsys_pcmc_monsbm_inc6789_20250724100445.uatv'  # source_file_name
        ]

        # Mock gcs_file_exists to return False for .transferred file and False for original file
        mock_gcs_file_exists.side_effect = [False, False]

        # Call the method and expect ValueError
        with pytest.raises(AirflowFailException, match="File not found at"):
            builder.validate_source_file(**dag_context_fixture)

    @patch('data_transfer.pcb_techops_tsys_outbound.AuditUtil')
    @patch('data_transfer.pcb_techops_tsys_outbound.deepcopy')
    def test_build_success(self, mock_deepcopy, mock_audit_util, dag_builder_fixture):
        """Test successful DAG build."""
        builder = dag_builder_fixture

        # Mock deepcopy
        mock_deepcopy.return_value = {
            "owner": "team-centaurs",
            'capability': 'Terminus Data Platform',
            'severity': 'P3',
            'sub_capability': 'Operations',
            'business_impact': 'N/A',
            'customer_impact': 'N/A',
            "email": [],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(seconds=10),
            "retry_exponential_backoff": True
        }

        # Mock audit util
        mock_audit_instance = MagicMock()
        mock_audit_util.return_value = mock_audit_instance

        # Call the build method with new interface
        dag = builder.build('test_dag_id', {'destination_bucket_name': 'tsys-outbound-pcb'})

        # Verify DAG was created
        assert dag is not None
        assert dag.dag_id == 'test_dag_id'

        # Verify tasks were created
        task_ids = [task.task_id for task in dag.tasks]
        expected_task_ids = [
            'start',
            'prepare_config',
            'validate_source_file',
            'move_data_from_staging_to_landing',
            'rename_source_file_with_transferred_suffix',
            'end'
        ]
        assert set(task_ids) == set(expected_task_ids)

        # Verify audit util was initialized
        mock_audit_util.assert_called_once_with('prepare_config', 'audit_record')
        assert builder.audit_util == mock_audit_instance

    def test_build_dag_structure(self, dag_builder_fixture):
        """Test DAG structure and dependencies."""
        builder = dag_builder_fixture

        # Mock dependencies
        builder.audit_util = MagicMock()
        builder.default_args = {
            "owner": "team-centaurs",
            'capability': 'Terminus Data Platform',
            'severity': 'P3',
            'sub_capability': 'Operations',
            'business_impact': 'N/A',
            'customer_impact': 'N/A',
            "email": [],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": "timedelta(seconds=10)",
            "retry_exponential_backoff": True
        }

        # Build DAG with new interface
        dag = builder.build('test_dag_id', {'destination_bucket_name': 'tsys-outbound-pcb'})

        # Verify DAG properties
        assert dag.render_template_as_native_obj is True
        assert dag.catchup is False
        assert dag.max_active_runs == 1
        assert dag.is_paused_upon_creation is True

        # Verify task dependencies
        start_task = dag.get_task('start')
        prepare_config_task = dag.get_task('prepare_config')
        validate_task = dag.get_task('validate_source_file')
        move_task = dag.get_task('move_data_from_staging_to_landing')
        rename_task = dag.get_task('rename_source_file_with_transferred_suffix')
        end_task = dag.get_task('end')

        # Check downstream tasks
        assert prepare_config_task in start_task.downstream_list
        assert validate_task in prepare_config_task.downstream_list
        assert move_task in validate_task.downstream_list
        assert rename_task in move_task.downstream_list
        assert end_task in rename_task.downstream_list

    def test_build_task_configurations(self, dag_builder_fixture):
        """Test task configurations and callbacks."""
        builder = dag_builder_fixture

        # Mock dependencies
        builder.audit_util = MagicMock()
        builder.default_args = {
            "owner": "team-centaurs",
            'capability': 'Terminus Data Platform',
            'severity': 'P3',
            'sub_capability': 'Operations',
            'business_impact': 'N/A',
            'customer_impact': 'N/A',
            "email": [],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": "timedelta(seconds=10)",
            "retry_exponential_backoff": True
        }

        # Build DAG with new interface
        dag = builder.build('test_dag_id', {'destination_bucket_name': 'tsys-outbound-pcb'})

        # Verify prepare_config task
        prepare_config_task = dag.get_task('prepare_config')
        assert prepare_config_task.python_callable == builder.prepare_config
        assert prepare_config_task.on_failure_callback == builder.audit_util.record_request_failure

        # Verify validate_source_file task
        validate_task = dag.get_task('validate_source_file')
        assert validate_task.python_callable == builder.validate_source_file
        assert validate_task.on_failure_callback == builder.audit_util.record_request_failure

        # Verify move_data_from_staging_to_landing task
        move_task = dag.get_task('move_data_from_staging_to_landing')
        assert move_task.on_failure_callback == builder.audit_util.record_request_failure
        assert move_task.on_success_callback == builder.audit_util.record_request_success

        # Verify rename_source_file_with_transferred_suffix task
        rename_task = dag.get_task('rename_source_file_with_transferred_suffix')
        assert rename_task.move_object is True
        assert rename_task.on_failure_callback == builder.audit_util.record_request_failure

    def test_build_template_variables(self, dag_builder_fixture):
        """Test that template variables are correctly set in tasks."""
        builder = dag_builder_fixture

        # Mock dependencies
        builder.audit_util = MagicMock()
        builder.default_args = {
            "owner": "team-centaurs",
            'capability': 'Terminus Data Platform',
            'severity': 'P3',
            'sub_capability': 'Operations',
            'business_impact': 'N/A',
            'customer_impact': 'N/A',
            "email": [],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": "timedelta(seconds=10)",
            "retry_exponential_backoff": True
        }

        # Build DAG with new interface
        dag = builder.build('test_dag_id', {'destination_bucket_name': 'tsys-outbound-pcb'})

        # Verify move task template variables
        move_task = dag.get_task('move_data_from_staging_to_landing')
        assert "{{ ti.xcom_pull(task_ids='prepare_config', key='source_bucket_name') }}" in str(move_task.source_bucket)
        assert "{{ ti.xcom_pull(task_ids='prepare_config', key='source_file_path') }}" in str(move_task.source_object)
        assert "{{ ti.xcom_pull(task_ids='prepare_config', key='destination_bucket') }}" in str(move_task.destination_bucket)
        assert "{{ ti.xcom_pull(task_ids='prepare_config', key='destination_file_path') }}" in str(move_task.destination_object)

        # Verify rename task template variables
        rename_task = dag.get_task('rename_source_file_with_transferred_suffix')
        assert "{{ ti.xcom_pull(task_ids='prepare_config', key='source_bucket_name') }}" in str(rename_task.source_bucket)
        assert "{{ ti.xcom_pull(task_ids='prepare_config', key='source_file_path') }}" in str(rename_task.source_object)
        assert "{{ ti.xcom_pull(task_ids='prepare_config', key='source_bucket_name') }}" in str(rename_task.destination_bucket)
        assert "{{ ti.xcom_pull(task_ids='prepare_config', key='source_file_path') }}.transferred" in str(rename_task.destination_object)
