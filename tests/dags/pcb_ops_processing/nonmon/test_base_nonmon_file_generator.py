import pytest
import pandas as pd
from unittest.mock import patch, MagicMock, call, mock_open
from abc import ABC
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pendulum
from google.cloud import storage, bigquery

# Import all functions and classes from the module
from pcb_ops_processing.nonmon.abc.base_nonmon_file_generator import (
    BaseNonmonFileGenerator,
    NonmonFileRegistry,
    RECORDS_PER_FILE,
    TRANSMISSION_ID,
    CLIENT_NUM,
    HEADER_IDNTFR,
    TRAILER_IDNTFR,
    NONMON_BODY_IDENTIFIER,
    TSYS_ID,
    JULIAN_DT,
    write_header,
    write_trailer,
    write_record,
    build_trailer_table,
    build_trailer_query,
    assemble_output_file_path,
    create_output_blob,
    create_audit_record,
    create_email,
    count_csv_rows
)
import util.constants as consts


class TestableBaseNonmonFileGenerator(BaseNonmonFileGenerator):
    """Concrete implementation of BaseNonmonFileGenerator for testing purposes"""

    def generate_nonmon_record(self, account_id, original_row=None) -> str:
        """Test implementation that returns a predictable record"""
        memo_texts = original_row[1:] if original_row else []
        return f"TEST_RECORD_{account_id}_{len(memo_texts) if memo_texts else 0}"

    def build_nonmon_table(self, table_id) -> str:
        """Test implementation that returns a simple DDL"""
        return f"CREATE TABLE {table_id} (id STRING, data STRING)"

    def build_nonmon_query(self, transformed_views: list, table_id: str) -> str:
        """Test implementation that returns a simple query"""
        return f"INSERT INTO {table_id} SELECT * FROM test_view"


@pytest.fixture
def mock_gcp_config():
    return {
        consts.DEPLOYMENT_ENVIRONMENT_NAME: 'dev',
        consts.LANDING_ZONE_PROJECT_ID: 'test-landing-project',
        consts.PROCESSING_ZONE_PROJECT_ID: 'test-processing-project'
    }


@pytest.fixture
def mock_config():
    return {
        'dag_id': 'test_nonmon_dag',
        'default_args': {
            'owner': 'test_owner',
            'retries': 1
        },
        'tasks': []
    }


@pytest.fixture
def mock_dag_context():
    """Mock DAG context for testing"""
    mock_dag = MagicMock()
    mock_dag.dag_id = "test_dag"
    mock_dag_run = MagicMock()
    mock_dag_run.conf = {
        "bucket": "test-bucket",
        "file_name": "test_file.csv",
        "folder_name": "test_folder",
        "name": "test_folder/test_file.csv"
    }
    mock_ti = MagicMock()
    return {
        'dag': mock_dag,
        'dag_run': mock_dag_run,
        'ti': mock_ti,
        'run_id': 'test_run_id'
    }


class TestStandaloneFunctions:
    """Test standalone functions in abc/base_nonmon_file_generator.py"""

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.create_output_blob')
    def test_write_header(self, mock_create_output_blob):
        """Test write_header function"""
        # Mock the blob creation
        mock_output_blob = MagicMock()
        mock_output_file = MagicMock()
        mock_output_blob.open.return_value = mock_output_file
        mock_staging_blob = MagicMock()
        mock_staging_file = MagicMock()
        mock_staging_blob.open.return_value = mock_staging_file

        mock_create_output_blob.side_effect = [
            (mock_output_blob, 'output_file.txt'),
            (mock_staging_blob, 'staging_file.txt')
        ]

        result = write_header(
            'dest-bucket', 'folder', 'test_file.txt', 'staging-bucket', 'utf-8'
        )

        assert isinstance(result, NonmonFileRegistry)
        assert result.output_file == mock_output_file
        assert result.output_filename == 'output_file.txt'
        assert result.staging_file == mock_staging_file
        assert result.staging_filename == 'staging_file.txt'

        # Check that header was written to both files
        assert mock_output_file.write.called
        assert mock_staging_file.write.called

    def test_write_trailer(self):
        """Test write_trailer function"""
        # Create mock file registry
        mock_output_file = MagicMock()
        mock_staging_file = MagicMock()
        file_registry = NonmonFileRegistry(
            output_file=mock_output_file,
            output_filename='output.txt',
            staging_file=mock_staging_file,
            staging_filename='staging.txt'
        )

        record_count = 1000
        write_trailer(file_registry, record_count, 'utf-8')

        # Verify trailer was written and files were closed
        mock_output_file.write.assert_called()
        mock_output_file.close.assert_called()
        mock_staging_file.write.assert_called()
        mock_staging_file.close.assert_called()

        # Check trailer content contains expected elements
        trailer_call = mock_output_file.write.call_args[0][0]
        assert TRANSMISSION_ID.encode('utf-8') in trailer_call
        assert b"000001000" in trailer_call  # Record count padded to 9 digits
        assert TRAILER_IDNTFR.encode('utf-8') in trailer_call

    def test_write_record(self):
        """Test write_record function"""
        mock_output_file = MagicMock()
        mock_staging_file = MagicMock()
        file_registry = NonmonFileRegistry(
            output_file=mock_output_file,
            output_filename='output.txt',
            staging_file=mock_staging_file,
            staging_filename='staging.txt'
        )

        test_record = "TEST_RECORD_DATA"
        write_record(file_registry, test_record, 'utf-8')

        # Verify record was written to both files
        mock_output_file.write.assert_called_with(test_record.encode('utf-8'))
        mock_staging_file.write.assert_called_with(test_record + "\n")

    def test_build_trailer_table(self):
        """Test build_trailer_table function"""
        table_id = 'test_project.test_dataset.trailer_table'
        result = build_trailer_table(table_id)

        assert f"CREATE TABLE IF NOT EXISTS `{table_id}`" in result
        assert "TRANSMISSION_ID STRING" in result
        assert "CLIENT_NUMBER STRING" in result
        assert "TOTAL_RECORD_COUNT STRING" in result

    def test_build_trailer_query(self):
        """Test build_trailer_query function"""
        transformed_views = [
            {'id': 'view1', 'columns': 'record_data, file_name'},
            {'id': 'view2', 'columns': 'record_data, file_name'}
        ]
        table_id = 'test_project.test_dataset.trailer_table'

        result = build_trailer_query(transformed_views, table_id)

        assert f"INSERT INTO {table_id}" in result
        assert "UNION ALL" in result
        assert "view1" in result
        assert "view2" in result

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.datetime')
    def test_assemble_output_file_path(self, mock_datetime):
        """Test assemble_output_file_path function"""
        from pathlib import Path
        mock_datetime.now.return_value.astimezone.return_value.strftime.return_value = '20240101120000'

        source_file_path = Path('input/test_file_inc123_20240101.csv')
        file_name_prefix = 'test_prefix'
        file_suffix = 'SUFFIX'

        # The function returns a tuple with 3 values based on the code
        destination_path, output_file_name, wildcard_path = assemble_output_file_path(
            source_file_path, file_name_prefix, file_suffix
        )

        assert 'test_prefix' in output_file_name
        assert 'inc123' in output_file_name
        assert file_suffix in output_file_name

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.storage.Client')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.datetime')
    def test_create_output_blob(self, mock_datetime, mock_storage_client):
        """Test create_output_blob function"""
        mock_datetime.now.return_value.astimezone.return_value.strftime.return_value = '20240101120000'

        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_storage_client.return_value.bucket.return_value = mock_bucket

        result_blob, result_filename = create_output_blob(
            'test-bucket', 'test/path', 'output_file.txt'
        )

        assert result_blob == mock_blob
        # The function adds timestamp to the filename
        assert 'output_file' in result_filename
        assert result_filename.endswith('.txt')
        mock_storage_client.assert_called_once()

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.create_data_transfer_audit_table_if_not_exists')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.assemble_output_file_path')
    def test_create_audit_record(self, mock_assemble_path, mock_create_table, mock_dag_context):
        """Test create_audit_record function"""
        mock_assemble_path.return_value = ('dest/path.txt', 'output.txt', 'dest/*.txt')
        mock_audit_util = MagicMock()

        config = {
            'destination_bucket': 'dest-bucket',
            'filename_prefix': 'test_prefix'
        }

        create_audit_record(
            mock_audit_util, config, 'dev', 'landing-proj', 'processing-proj',
            **mock_dag_context
        )

        mock_create_table.assert_called_once()
        mock_audit_util.record_request_received.assert_called_once()

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_file_env')
    def test_create_email(self, mock_read_file, mock_dag_context):
        """Test create_email function"""
        mock_read_file.return_value = '<html>Template: {{file_count}}</html>'
        mock_dag_context['ti'].xcom_pull.side_effect = [
            10,  # file_row_count from create_nonmon_files_task
            ['gs://dest-bucket/file1.txt', 'gs://dest-bucket/file2.txt']  # output_file_list from create_nonmon_files_task
        ]

        config = {
            'email': {
                'subject': 'Test Subject',
                'recipients': {'dev': 'test@example.com'},
                'description': 'Test Description',
                'approver': 'Test Approver',
                'purpose': 'Test Purpose'
            }
        }

        create_email(config, 'dev', '/test/config', **mock_dag_context)

        # Check XCom pushes
        mock_dag_context['ti'].xcom_push.assert_any_call(
            key=consts.SUBJECT, value='Test Subject'
        )
        mock_dag_context['ti'].xcom_push.assert_any_call(
            key=consts.RECIPIENTS, value='test@example.com'
        )

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.storage.Client')
    def test_count_csv_rows(self, mock_storage_client, mock_dag_context):
        """Test count_csv_rows function"""
        # Mock file content with header + 250 rows
        file_lines = ['header1,header2'] + [f'row{i},value{i}' for i in range(250)]

        mock_blob = MagicMock()
        mock_blob.open.return_value.__enter__.return_value = file_lines
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_storage_client.return_value.bucket.return_value = mock_bucket

        config = {'staging_bucket': 'test-staging'}
        source_path = 'test/file.csv'

        count_csv_rows(config, source_path, **mock_dag_context)

        # Verify XCom pushes
        mock_dag_context['ti'].xcom_push.assert_any_call(key='total_rows', value=250)
        expected_batches = (250 + RECORDS_PER_FILE - 1) // RECORDS_PER_FILE
        mock_dag_context['ti'].xcom_push.assert_any_call(key='num_batches', value=expected_batches)


class TestBaseNonmonFileGenerator:
    """Test cases for BaseNonmonFileGenerator abstract base class"""

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.AuditUtil')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_init_initializes_correctly(self, mock_settings, mock_read_yaml, mock_read_var, mock_audit_util,
                                        mock_gcp_config, mock_config):
        """Test that BaseNonmonFileGenerator initializes with correct attributes"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'
        mock_audit_util.return_value = MagicMock()

        generator = TestableBaseNonmonFileGenerator('test_config.yaml')

        assert generator.gcp_config == mock_gcp_config
        assert generator.config == mock_config
        assert generator.deploy_env == 'dev'
        assert generator.landing_project_id == 'test-landing-project'
        assert generator.processing_project_id == 'test-processing-project'
        assert generator.config_dir == '/test/dags/pcb_ops_processing/nonmon/config'
        assert generator.local_tz.name == 'America/Toronto'

    def test_cannot_instantiate_abstract_class(self):
        """Test that BaseNonmonFileGenerator cannot be instantiated directly"""
        with pytest.raises(TypeError):
            BaseNonmonFileGenerator('test_config.yaml')

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.AuditUtil')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_abstract_methods_implemented(self, mock_settings, mock_read_yaml, mock_read_var, mock_audit_util,
                                          mock_gcp_config, mock_config):
        """Test that abstract methods are properly implemented in concrete class"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'
        mock_audit_util.return_value = MagicMock()

        generator = TestableBaseNonmonFileGenerator('test_config.yaml')

        # Test abstract methods
        record_result = generator.generate_nonmon_record('12345', ['12345', 'memo1', 'memo2'])
        table_result = generator.build_nonmon_table('test_table')
        query_result = generator.build_nonmon_query([], 'test_table')

        assert record_result == "TEST_RECORD_12345_2"
        assert table_result == "CREATE TABLE test_table (id STRING, data STRING)"
        assert query_result == "INSERT INTO test_table SELECT * FROM test_view"

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.write_trailer')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.write_record')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.write_header')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.storage.Client')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.AuditUtil')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_create_nonmon_files(self, mock_settings, mock_read_yaml, mock_read_var, mock_audit_util,
                                 mock_storage_client, mock_write_header, mock_write_record, mock_write_trailer,
                                 mock_gcp_config, mock_config, mock_dag_context):
        """Test create_nonmon_files method"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'
        mock_audit_util.return_value = MagicMock()

        # Mock file registry
        mock_file_registry = NonmonFileRegistry(
            output_file=MagicMock(),
            output_filename='output_20250101120000.txt',
            staging_file=MagicMock(),
            staging_filename='staging_20250101120000.txt'
        )
        mock_write_header.return_value = mock_file_registry

        # Mock storage operations
        mock_blob = MagicMock()
        mock_blob.open.return_value.__enter__.return_value = [
            'account_id,memo1,memo2',
            '12345,test memo,another memo',
            '67890,memo text,more text'
        ]
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_storage_client.return_value.bucket.return_value = mock_bucket

        # Mock XCom pulls
        mock_dag_context['ti'].xcom_pull.side_effect = [2, 1]  # total_rows, num_batches

        generator = TestableBaseNonmonFileGenerator('test_config.yaml')
        config = {
            consts.STAGING_BUCKET: 'staging-bucket',
            consts.DESTINATION_BUCKET: 'dest-bucket',
            consts.CHARACTER_SET: 'utf-8'
        }

        generator.create_nonmon_files(config, 'source/path.csv', 'dest/path.txt', 'output.txt', **mock_dag_context)

        # Verify header, records, and trailer were written
        mock_write_header.assert_called()
        assert mock_write_record.call_count == 2  # Two data rows
        mock_write_trailer.assert_called()

        # Verify XCom pushes
        mock_dag_context['ti'].xcom_push.assert_any_call(key='file_row_count', value=2)
        # Check that output_file_list and staging_file_list were pushed (actual values will contain the file paths)
        xcom_calls = mock_dag_context['ti'].xcom_push.call_args_list
        output_file_list_call = [c for c in xcom_calls if c.kwargs.get('key') == 'output_file_list']
        staging_file_list_call = [c for c in xcom_calls if c.kwargs.get('key') == 'staging_file_list']

        assert len(output_file_list_call) == 1
        assert len(staging_file_list_call) == 1
        assert 'gs://dest-bucket/dest/' in output_file_list_call[0].kwargs['value'][0]
        assert 'gs://staging-bucket/dest/' in staging_file_list_call[0].kwargs['value'][0]

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.apply_column_transformation')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.create_external_table')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.bigquery.Client')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.AuditUtil')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_write_to_bq(self, mock_settings, mock_read_yaml, mock_read_var, mock_audit_util,
                         mock_bq_client, mock_create_external_table, mock_apply_column_transformation,
                         mock_gcp_config, mock_config, mock_dag_context):
        """Test write_to_bq method"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'
        mock_audit_util.return_value = MagicMock()
        mock_bq_client.return_value = MagicMock()

        # Mock external table and column transformation
        mock_create_external_table.return_value = 'test-dataset.FILE1_DBEXT'
        mock_apply_column_transformation.return_value = {
            'id': 'test-dataset.FILE1_DBEXT_COLUMN_TF',
            'columns': 'record_data, file_name'
        }

        # Mock XCom pull for staging_file_list
        mock_dag_context['ti'].xcom_pull.return_value = ['gs://staging-bucket/folder/staging_file.txt']

        generator = TestableBaseNonmonFileGenerator('test_config.yaml')
        config = {
            consts.BIGQUERY: {
                consts.PROJECT_ID: 'test-project',
                consts.DATASET_ID: 'test-dataset',
                consts.TABLE_NAME: 'test-table',
                consts.TRAILER_SEGMENT_NAME: {
                    consts.TABLE_NAME: 'test-trailer-table'
                }
            }
        }

        generator.write_to_bq(config, **mock_dag_context)

        # Verify BigQuery client was called
        mock_bq_client.assert_called()
        mock_create_external_table.assert_called()
        mock_apply_column_transformation.assert_called()

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.AuditUtil')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.DAG_DEFAULT_ARGS', {})
    def test_create_dag(self, mock_settings, mock_read_yaml, mock_read_var, mock_audit_util,
                        mock_gcp_config):
        """Test that create_dag creates a DAG with correct configuration"""
        mock_config = {
            'default_args': {
                'owner': 'test_owner',
                'retries': 2
            }
        }
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'
        mock_audit_util.return_value = MagicMock()

        generator = TestableBaseNonmonFileGenerator('test_config.yaml')
        dag = generator.create_dag('test_dag_id', mock_config)

        assert isinstance(dag, DAG)
        assert dag.dag_id == 'test_dag_id'
        assert dag.render_template_as_native_obj is True
        assert dag.is_paused_upon_creation is True
        assert dag.max_active_runs == 1
        assert dag.catchup is False

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.AuditUtil')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_create_method(self, mock_settings, mock_read_yaml, mock_read_var, mock_audit_util):
        """Test the create method that returns DAGs dictionary"""
        mock_gcp_config = {
            consts.DEPLOYMENT_ENVIRONMENT_NAME: 'dev',
            consts.LANDING_ZONE_PROJECT_ID: 'test-landing-project',
            consts.PROCESSING_ZONE_PROJECT_ID: 'test-processing-project'
        }
        mock_config = {
            'job1': {'default_args': {'owner': 'test'}},
            'job2': {'default_args': {'owner': 'test'}}
        }
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'
        mock_audit_util.return_value = MagicMock()

        generator = TestableBaseNonmonFileGenerator('test_config.yaml')
        result = generator.create()

        assert isinstance(result, dict)
        assert 'job1' in result
        assert 'job2' in result
        assert isinstance(result['job1'], DAG)
        assert isinstance(result['job2'], DAG)


class TestErrorHandling:
    """Test error handling scenarios"""

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.AuditUtil')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_handles_missing_gcp_config_keys(self, mock_settings, mock_read_yaml, mock_read_var, mock_audit_util):
        """Test handling of missing keys in GCP config"""
        incomplete_gcp_config = {
            consts.DEPLOYMENT_ENVIRONMENT_NAME: 'dev'
            # Missing LANDING_ZONE_PROJECT_ID and PROCESSING_ZONE_PROJECT_ID
        }
        mock_read_var.return_value = incomplete_gcp_config
        mock_read_yaml.return_value = {}
        mock_settings.DAGS_FOLDER = '/test/dags'
        mock_audit_util.return_value = MagicMock()

        generator = TestableBaseNonmonFileGenerator('test_config.yaml')

        assert generator.landing_project_id is None
        assert generator.processing_project_id is None
        assert generator.deploy_env == 'dev'

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.AuditUtil')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_handles_empty_config(self, mock_settings, mock_read_yaml, mock_read_var, mock_audit_util):
        """Test handling of empty configuration"""
        mock_read_var.return_value = {}
        mock_read_yaml.return_value = {}
        mock_settings.DAGS_FOLDER = '/test/dags'
        mock_audit_util.return_value = MagicMock()

        generator = TestableBaseNonmonFileGenerator('test_config.yaml')

        assert generator.deploy_env is None
        assert generator.landing_project_id is None
        assert generator.processing_project_id is None


class TestConstants:
    """Test that constants are properly defined"""

    def test_constants_exist(self):
        """Test that all required constants are defined"""
        assert RECORDS_PER_FILE == 100_000
        assert TRANSMISSION_ID == "I9999STD"
        assert CLIENT_NUM == "7607"
        assert HEADER_IDNTFR == "SINFHDR"
        assert TRAILER_IDNTFR == "SINFTRL"
        assert NONMON_BODY_IDENTIFIER == "IIIIII"
        assert TSYS_ID == "SDDATA"
