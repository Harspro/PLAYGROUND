import pytest
from unittest.mock import patch, call, MagicMock
from dataclasses import asdict
from data_transfer.shared_drive_data_transfer.ctera_shared_drive_data_export_job import move_data_to_remote_server, prepare_config, ConnectionInfo, check_file_transfer_status
from airflow.exceptions import AirflowException
from util.auditing_utils.model.audit_record import AuditRecord
from util.miscutils import sanitize_string


@pytest.fixture
def dag_context_fixture() -> dict:
    mock_dag = MagicMock()
    mock_dag.dag_id = "ctera_shared_drive_data_export_job"

    mock_dag_run = MagicMock()
    mock_dag_run.conf = {
        "dag_id": "ctera_shared_drive_data_export_job",
        "remote_filepath": "testing",
        "lookup_key": ["dfspcbalmuat", "nonprod"],
        "marker_file_bucket": "terminus-pds-np",
        "marker_file_prefix": "data_transfer/export/artifacts/bigquery.png.17af6ccb-2ddf-4c63-88b2-8793262841e6",
        "source_bucket": "terminus-pds-np",
        "source_dag_id": "shared_drive_data_export_example",
        "source_dag_run_id": "manual__2025-02-11T16:13:09+00:00",
        "source_path": "tmp/bigquery.png",
        "source_project_id": "pcb-nonprod-pds",
        "source_type": "file",
        "team_name": "pds",
        "trigger_type": "manual"
    }

    mock_task_instance = MagicMock()
    mock_task_instance.xcom_push.return_value = None

    return {
        'dag_run': mock_dag_run,
        'dag': mock_dag,
        'ti': mock_task_instance,
        'run_id': 'manual__2025-02-11T16:15:10+00:00'
    }


@pytest.fixture
def audit_record_fixture() -> AuditRecord:
    expected_audit_record_values = """
        'pcb-nonprod-pds',
        'shared_drive_data_export_example',
        'manual__2025-02-11T16:13:09+00:00',
        'file',
        'gs://terminus-pds-np/tmp/bigquery.png',
        'bigquery.png',
        'pcb-nonprod-pds',
        'testing',
        'ctera_shared_drive_data_export_job',
        'manual__2025-02-11T16:15:10+00:00',
        'Shared_Drive_Data_Export',
        'manual',
        'pcb-nonprod-pds',
        CURRENT_TIMESTAMP()
    """

    audit_record = AuditRecord(dataset_id='domain_technical',
                               table_id='DATA_TRANSFER_AUDIT',
                               audit_record_values=expected_audit_record_values)

    return audit_record


class TestCteraSharedDriveExportJob:

    @patch('data_transfer.shared_drive_data_transfer.ctera_shared_drive_data_export_job.read_yamlfile_env')
    @patch('data_transfer.shared_drive_data_transfer.ctera_shared_drive_data_export_job.SMBUtil')
    @patch('data_transfer.shared_drive_data_transfer.ctera_shared_drive_data_export_job.resolve_smb_server_config')
    def test_move_data_to_remote_server(
        self, mock_resolve_smb_server_configg, mock_smbutil, mock_read_yamlfile_env
    ):
        # Mock the resolve_smb_server_config to return specific values
        mock_resolve_smb_server_configg.return_value = ('10.59.196.19', 'LCE', 'SVCPCBALMSHAREAPIUAT')

        # Mocking complete class SMBUtil using MagicMock
        mock_ctera_util = MagicMock()
        mock_smbutil.return_value = mock_ctera_util

        # Setting the return value for the mocked objects.
        mock_read_yamlfile_env.return_value = {
            'dfspcbalmuat': {
                'nonprod': {
                    'ctera_shared_folder': 'DFSPCBALMUAT/UAT/poc_data_movement'
                }
            }
        }

        connection_info = {
            'destination': 'dfspcbalmuat',
            'environment': 'nonprod'
        }

        # Simulate that the directory doesn't exist and make_dir_recursively should be called
        mock_ctera_util.is_dir.return_value = False
        mock_ctera_util.copy_gcs_to_ctera.return_value = 'SUCCESS'

        # Call the move_data_to_remote_server function
        move_data_to_remote_server(
            remote_filepath='testing',
            destination_file_name='bigquery.png',
            source_bucket='terminus-pds-np',
            source_path='tmp/bigquery.png',
            connection_info=connection_info
        )

        # Verify that the SMBUtil methods were called as expected
        mock_smbutil.assert_called_once_with('10.59.196.19', 'LCE', 'SVCPCBALMSHAREAPIUAT')
        mock_ctera_util.make_dir_recursively.assert_called()  # Ensure make_dir_recursively was called to create directories because we have made is dir as False
        mock_ctera_util.copy_gcs_to_ctera.assert_called_once_with(
            'DFSPCBALMUAT/UAT/poc_data_movement/testing', 'bigquery.png', 'terminus-pds-np', None, 'tmp/bigquery.png'
        )

        # setting invalid connection info that will trigger airflow exception
        connection_info = {
            'destination': 'invalid_folder',
            'environment': 'nonprod'
        }

        # Call the function and assert that AirflowException is raised

        with pytest.raises(AirflowException) as exc_info:
            move_data_to_remote_server(
                remote_filepath='testing',
                destination_file_name='bigquery.png',
                source_bucket='terminus-pds-np',
                source_path='tmp/bigquery.png',
                connection_info=connection_info
            )

            # Check that the exception message is correct
            assert ("The lookup key invalid_folder is not found." in str(exc_info.exception))

        # setting invalid connection info that will trigger airflow exception
        connection_info = {
            'destination': 'dfspcbalmuat',
            'environment': 'invalid_environment'
        }

        # Call the function and assert that AirflowException is raised
        with pytest.raises(AirflowException) as exc_info:
            move_data_to_remote_server(
                remote_filepath='testing',
                destination_file_name='bigquery.png',
                source_bucket='terminus-pds-np',
                source_path='tmp/bigquery.png',
                connection_info=connection_info
            )

            # Check that the exception message is correct
            assert ("The lookup key invalid_environment is not found." in str(exc_info.exception))

    @patch('data_transfer.shared_drive_data_transfer.ctera_shared_drive_data_export_job.AuditUtil')
    @patch('data_transfer.shared_drive_data_transfer.ctera_shared_drive_data_export_job.AuditUtil.record_request_received')
    @patch('data_transfer.shared_drive_data_transfer.ctera_shared_drive_data_export_job.create_data_transfer_audit_table_if_not_exists')
    def test_prepare_config(
        self, mock_create_audit_table, mock_record_request_received, mock_audit_util, dag_context_fixture: dict, audit_record_fixture: AuditRecord
    ):
        # Call the prepare_config function
        prepare_config(**dag_context_fixture)

        mock_create_audit_table.assert_called_once()

        mock_record_request_received.assert_called_once()
        audit_record_call_arg = mock_record_request_received.call_args.args[0]
        assert (audit_record_call_arg.dataset_id == audit_record_fixture.dataset_id)
        assert (audit_record_call_arg.table_id == audit_record_fixture.table_id)
        assert (sanitize_string(audit_record_call_arg.audit_record_values) == sanitize_string(audit_record_fixture.audit_record_values))

        mock_ti = dag_context_fixture['ti']

        marker_file_bucket_call = call.xcom_push(key='marker_file_bucket', value='terminus-pds-np')
        marker_file_prefix_call = call.xcom_push(key='marker_file_prefix', value='data_transfer/export/artifacts/bigquery.png.17af6ccb-2ddf-4c63-88b2-8793262841e6')
        connect_info_call = call.xcom_push(key='connection_info', value={"destination": "dfspcbalmuat", "environment": "nonprod"})
        audit_record_call = call.xcom_push(key='audit_record', value=asdict(audit_record_call_arg))
        source_bucket_call = call.xcom_push(key='source_bucket', value='terminus-pds-np')
        source_path_call = call.xcom_push(key='source_path', value='tmp/bigquery.png')
        remote_filepath_call = call.xcom_push(key='remote_filepath', value='testing')
        destination_file_name_call = call.xcom_push(key='destination_file_name', value='bigquery.png')

        mock_ti.assert_has_calls([marker_file_bucket_call, marker_file_prefix_call, connect_info_call, audit_record_call,
                                  source_bucket_call, source_path_call, remote_filepath_call, destination_file_name_call])

    def test_check_file_transfer_status(self):

        def get_context_with_state(state: str):
            dummy_ti = MagicMock()
            dummy_ti.state = state
            dummy_ti.task_id = 'move_data_to_remote_server'

            dummy_dag_run = MagicMock()
            dummy_dag_run.get_task_instance.return_value = dummy_ti

            return {
                'dag_run': dummy_dag_run
            }

        context_success = get_context_with_state('success')
        check_file_transfer_status(**context_success)

        context_failed = get_context_with_state('failed')
        with pytest.raises(AirflowException, match="Task move_data_to_remote_server failed. Hence, Failing the DAG."):
            check_file_transfer_status(**context_failed)

        context_skipped = get_context_with_state('skipped')
        with pytest.raises(AirflowException, match="Task move_data_to_remote_server failed. Hence, Failing the DAG."):
            check_file_transfer_status(**context_skipped)

        context_upstream_failed = get_context_with_state('upstream_failed')
        with pytest.raises(AirflowException, match="Task move_data_to_remote_server failed. Hence, Failing the DAG."):
            check_file_transfer_status(**context_upstream_failed)
