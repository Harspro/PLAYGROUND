from unittest.mock import patch, MagicMock, Mock
import pytest
import sys
import os
from airflow.exceptions import AirflowFailException
from datetime import datetime
from google.cloud.bigquery import SchemaField

# Add the dags directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'dags'))

# Import the module to ensure it's available for patching
try:
    import fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job  # noqa: F401
except ImportError:
    pass  # Will be imported when needed


@pytest.fixture
def mock_module():
    """Mock module dependencies"""
    mock_constants = MagicMock()
    mock_constants.PROCESSING_ZONE_PROJECT_ID = 'processing_zone_project_id'

    # Mock the util module and its submodules
    mock_util = MagicMock()
    mock_util.constants = mock_constants

    # Create proper mocks for bq_utils functions
    mock_bq_utils = MagicMock()
    mock_bq_utils.create_external_table = MagicMock()
    mock_bq_utils.run_bq_query = MagicMock()
    mock_bq_utils.submit_transformation = MagicMock()
    mock_util.bq_utils = mock_bq_utils

    # Create a mock BaseDagBuilder class that can be inherited from
    class MockBaseDagBuilder:
        def __init__(self, environment_config):
            self.environment_config = environment_config

        def prepare_default_args(self, default_args):
            return default_args

    with patch.dict('sys.modules', {
        'util': mock_util,
        'util.constants': mock_constants,
        'util.bq_utils': mock_util.bq_utils,
        'google.cloud.bigquery': MagicMock(),
        'google.cloud.storage': MagicMock(),
        'airflow': MagicMock(),
        'airflow.operators.python': MagicMock(),
        'airflow.operators.empty': MagicMock(),
        'airflow.operators.trigger_dagrun': MagicMock(),
        'airflow.exceptions': MagicMock(AirflowFailException=AirflowFailException),
        'dag_factory.abc': MagicMock(BaseDagBuilder=MockBaseDagBuilder),
    }):
        yield


@pytest.fixture
def mock_environment_config():
    """Mock environment configuration"""
    mock_config = MagicMock()
    mock_config.local_tz = None
    mock_config.gcp_config = {'processing_zone_project_id': 'test-processing-project'}
    mock_config.deploy_env = 'test'
    return mock_config


@pytest.fixture
def mock_config():
    """Mock DAG configuration"""
    return {
        'bigquery_config': {
            'project_id_template': 'pcb-{env}-landing',
            'dataset_id': 'domain_fraud_risk_intelligence',
            'table_name': 'FRAUD_RISK_TRUTH_DATA_INBOUND',
            'default_env': 'uat'
        },
        'staging_config': {
            'dataset_id': 'domain_customer_acquisition',
            'table_name': 'fraud_risk_truth_data_staging',
            'file_format': 'CSV',
            'field_delimiter': ',',
            'skip_leading_rows': 1,
            'expiration_hours': 6
        },
        'validation_config': {
            'required_columns': ['request_id', 'action', 'final_review_status', 'event_classification', 'event_tag'],
            'mandatory_fields': ['request_id', 'action', 'final_review_status', 'event_classification', 'event_tag']
        },
        'column_mapping': {
            'request_id': 'RequestId',
            'action': 'Action',
            'final_review_status': 'FinalReviewStatus',
            'event_classification': 'EventClassification',
            'event_tag': 'EventTag'
        },
        'metadata_fields': [
            {'target': 'SourceFilename', 'type': 'STRING', 'value': 'file_name'},
            {'target': 'INGESTION_TIMESTAMP', 'type': 'TIMESTAMP', 'value': 'CURRENT_TIMESTAMP()'}
        ],
        'dag': {
            'description': 'Test DAG',
            'schedule': None,
            'max_active_runs': 1,
            'is_paused_upon_creation': True,
            'tags': ['test'],
            'render_template_as_native_obj': True,
            'task_id': {
                'kafka_writer_task_id': 'trigger_kafka_processing_dag'
            },
            'dag_id': {
                'kafka_trigger_dag_id': 'fraud_risk_truthdata_kafka_processing'
            },
            'wait_for_completion': True,
            'poke_interval': 30
        },
        'default_args': {
            'catchup': False
        }
    }


@pytest.fixture
def mock_dag_context(mock_config):
    """Mock DAG run context"""
    mock_ti = MagicMock()
    mock_dag_run = MagicMock()
    mock_dag_run.conf = {
        'bucket': 'test-bucket',
        'name': 'test-file.csv',
        'env': 'uat'
    }

    context = {
        'config': mock_config,
        'dag_run': mock_dag_run,
        'ti': mock_ti
    }
    return context


class TestReadCsvHeadersFromGcs:
    """Test cases for read_csv_headers_from_gcs function"""

    @patch('fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job.storage.Client')
    def test_read_csv_headers_success(self, mock_storage_client):
        """Test successful reading of CSV headers"""
        from fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job import read_csv_headers_from_gcs

        # Setup mocks
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        # Mock CSV header content
        csv_content = b'request_id,action,final_review_status,event_classification,event_tag\n'
        mock_blob.download_as_bytes.return_value = csv_content

        # Execute
        headers = read_csv_headers_from_gcs('test-bucket', 'test-file.csv')

        # Assert
        assert headers == ['request_id', 'action', 'final_review_status', 'event_classification', 'event_tag']
        mock_blob.download_as_bytes.assert_called_once_with(end=8192)

    @patch('fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job.storage.Client')
    def test_read_csv_headers_with_bom(self, mock_storage_client):
        """Test reading CSV headers with BOM character"""
        from fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job import read_csv_headers_from_gcs

        # Setup mocks
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        # Mock CSV header content with BOM
        csv_content = b'\xef\xbb\xbfrequest_id,action\n'
        mock_blob.download_as_bytes.return_value = csv_content

        # Execute
        headers = read_csv_headers_from_gcs('test-bucket', 'test-file.csv')

        # Assert
        assert headers == ['request_id', 'action']
        assert '\ufeff' not in headers[0]

    @patch('fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job.storage.Client')
    def test_read_csv_headers_failure(self, mock_storage_client):
        """Test failure when reading CSV headers"""
        from fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job import read_csv_headers_from_gcs

        # Setup mocks to raise exception
        mock_storage_client.side_effect = Exception("GCS connection failed")

        # Execute and assert
        with pytest.raises(AirflowFailException) as exc_info:
            read_csv_headers_from_gcs('test-bucket', 'test-file.csv')

        assert "Failed to read CSV header" in str(exc_info.value)


class TestFraudRiskTruthDataDagBuilder:
    """Test cases for FraudRiskTruthDataDagBuilder class"""

    def test_get_bq_table_id_default_env(self, mock_module, mock_environment_config):
        """Test getting BigQuery table ID with default environment"""
        from fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job import FraudRiskTruthDataDagBuilder

        builder = FraudRiskTruthDataDagBuilder(mock_environment_config)
        config = {
            'bigquery_config': {
                'project_id_template': 'pcb-{env}-landing',
                'dataset_id': 'test_dataset',
                'table_name': 'test_table',
                'default_env': 'uat'
            }
        }

        result = builder.get_bq_table_id(config)
        assert result == 'pcb-uat-landing.test_dataset.test_table'

    def test_get_bq_table_id_custom_env(self, mock_module, mock_environment_config):
        """Test getting BigQuery table ID with custom environment"""
        from fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job import FraudRiskTruthDataDagBuilder

        builder = FraudRiskTruthDataDagBuilder(mock_environment_config)
        config = {
            'bigquery_config': {
                'project_id_template': 'pcb-{env}-landing',
                'dataset_id': 'test_dataset',
                'table_name': 'test_table',
                'default_env': 'uat'
            }
        }

        result = builder.get_bq_table_id(config, 'prod')
        assert result == 'pcb-prod-landing.test_dataset.test_table'

    @patch('fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job.read_csv_headers_from_gcs')
    @patch('fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job.create_external_table')
    @patch('fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job.bigquery.Client')
    @patch('fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job.FraudRiskTruthDataFileValidation')
    def test_file_validation_task_success(
        self,
        mock_validation_class,
        mock_bq_client,
        mock_create_external_table,
        mock_read_headers,
        mock_module,
        mock_environment_config,
        mock_config,
        mock_dag_context
    ):
        """Test successful file validation task"""
        from fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job import FraudRiskTruthDataDagBuilder

        # Setup mocks
        builder = FraudRiskTruthDataDagBuilder(mock_environment_config)
        mock_validation = MagicMock()
        mock_validation_class.return_value = mock_validation
        mock_validation.validate_file_format.return_value = {'is_valid': True}
        mock_validation.perform_file_validation.return_value = {
            'overall_valid': True,
            'column_validation': {'is_valid': True},
            'field_validation': {'is_valid': True}
        }

        mock_bq_client_instance = MagicMock()
        mock_bq_client.return_value = mock_bq_client_instance

        mock_read_headers.return_value = ['request_id', 'action', 'final_review_status', 'event_classification', 'event_tag']

        # Execute
        result = builder._file_validation_task(**mock_dag_context)

        # Assert
        assert "SUCCESS" in result
        mock_validation.validate_file_format.assert_called_once()
        mock_validation.perform_file_validation.assert_called_once()
        mock_create_external_table.assert_called_once()
        # Verify xcom_push was called with staging_table_id, file_name, and env
        # Check that xcom_push was called at least 3 times (staging_table_id, file_name, env)
        assert mock_dag_context['ti'].xcom_push.call_count >= 3
        # Verify staging_table_id was pushed (value contains timestamp, so check pattern)
        staging_table_calls = [call for call in mock_dag_context['ti'].xcom_push.call_args_list if len(call) > 1 and call[1].get('key') == 'staging_table_id']
        assert len(staging_table_calls) > 0
        staging_table_id = staging_table_calls[0][1]['value']
        assert 'test-processing-project.domain_customer_acquisition.fraud_risk_truth_data_staging_' in staging_table_id
        # Verify file_name and env were pushed
        mock_dag_context['ti'].xcom_push.assert_any_call(key='file_name', value='test-file.csv')
        mock_dag_context['ti'].xcom_push.assert_any_call(key='env', value='uat')

    @patch('fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job.FraudRiskTruthDataFileValidation')
    def test_file_validation_task_no_dag_run_conf(
        self,
        mock_validation_class,
        mock_module,
        mock_environment_config,
        mock_config
    ):
        """Test file validation task fails when no DAG run configuration"""
        from fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job import FraudRiskTruthDataDagBuilder

        builder = FraudRiskTruthDataDagBuilder(mock_environment_config)
        context = {
            'config': mock_config,
            'dag_run': None,
            'ti': MagicMock()
        }

        with pytest.raises(AirflowFailException) as exc_info:
            builder._file_validation_task(**context)

        assert "No DAG run configuration found" in str(exc_info.value)

    @patch('fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job.FraudRiskTruthDataFileValidation')
    def test_file_validation_task_missing_bucket_or_file(
        self,
        mock_validation_class,
        mock_module,
        mock_environment_config,
        mock_config
    ):
        """Test file validation task fails when bucket or file name is missing"""
        from fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job import FraudRiskTruthDataDagBuilder

        builder = FraudRiskTruthDataDagBuilder(mock_environment_config)
        context = {
            'config': mock_config,
            'dag_run': MagicMock(),
            'ti': MagicMock()
        }
        context['dag_run'].conf = {'bucket': 'test-bucket'}  # Missing file name

        with pytest.raises(AirflowFailException) as exc_info:
            builder._file_validation_task(**context)

        assert "Bucket and file name must be provided" in str(exc_info.value)

    @patch('fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job.FraudRiskTruthDataFileValidation')
    def test_file_validation_task_file_format_validation_fails(
        self,
        mock_validation_class,
        mock_module,
        mock_environment_config,
        mock_config,
        mock_dag_context
    ):
        """Test file validation task fails when file format validation fails"""
        from fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job import FraudRiskTruthDataDagBuilder

        builder = FraudRiskTruthDataDagBuilder(mock_environment_config)
        mock_validation = MagicMock()
        mock_validation_class.return_value = mock_validation
        mock_validation.validate_file_format.return_value = {
            'is_valid': False,
            'error_message': 'File is not CSV'
        }

        with pytest.raises(AirflowFailException) as exc_info:
            builder._file_validation_task(**mock_dag_context)

        assert "File validation failed" in str(exc_info.value)
        assert "File is not CSV" in str(exc_info.value)

    @patch('fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job.read_csv_headers_from_gcs')
    @patch('fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job.create_external_table')
    @patch('fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job.bigquery.Client')
    @patch('fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job.FraudRiskTruthDataFileValidation')
    def test_file_validation_task_validation_fails(
        self,
        mock_validation_class,
        mock_bq_client,
        mock_create_external_table,
        mock_read_headers,
        mock_module,
        mock_environment_config,
        mock_config,
        mock_dag_context
    ):
        """Test file validation task fails when validation fails"""
        from fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job import FraudRiskTruthDataDagBuilder

        builder = FraudRiskTruthDataDagBuilder(mock_environment_config)
        mock_validation = MagicMock()
        mock_validation_class.return_value = mock_validation
        mock_validation.validate_file_format.return_value = {'is_valid': True}
        mock_validation.perform_file_validation.return_value = {
            'overall_valid': False,
            'column_validation': {
                'is_valid': False,
                'error_message': 'Missing required columns'
            },
            'field_validation': {'is_valid': True}
        }

        mock_bq_client_instance = MagicMock()
        mock_bq_client.return_value = mock_bq_client_instance
        mock_read_headers.return_value = ['request_id', 'action']

        with pytest.raises(AirflowFailException) as exc_info:
            builder._file_validation_task(**mock_dag_context)

        assert "File validation failed" in str(exc_info.value)

    @patch('fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job.run_bq_query')
    @patch('fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job.bigquery.Client')
    def test_load_data_to_bigquery_task_success(
        self,
        mock_bq_client,
        mock_run_bq_query,
        mock_module,
        mock_environment_config,
        mock_config
    ):
        """Test successful data loading to BigQuery"""
        from fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job import FraudRiskTruthDataDagBuilder

        builder = FraudRiskTruthDataDagBuilder(mock_environment_config)

        # Setup mocks
        mock_ti = MagicMock()
        mock_ti.xcom_pull.side_effect = lambda key, task_ids: {
            'staging_table_id': 'test-project.test-dataset.staging_table',
            'file_name': 'path/to/test-file.csv',
            'env': 'uat'
        }.get(key)

        mock_client = MagicMock()
        mock_bq_client.return_value = mock_client

        # Mock table schema
        mock_table = MagicMock()
        mock_table.schema = [
            SchemaField('request_id', 'STRING'),
            SchemaField('action', 'STRING'),
            SchemaField('event_tag', 'STRING')
        ]
        mock_table.num_rows = 100
        mock_client.get_table.return_value = mock_table

        context = {
            'config': mock_config,
            'ti': mock_ti
        }

        # Execute
        result = builder._load_data_to_bigquery_task(**context)

        # Assert
        assert "SUCCESS" in result
        assert "100 rows" in result
        mock_run_bq_query.assert_called_once()
        mock_ti.xcom_push.assert_called_once_with(key='rows_loaded', value=100)

        # Verify INSERT query structure
        query = mock_run_bq_query.call_args[0][0]
        assert 'INSERT INTO' in query
        assert 'SELECT' in query
        assert 'KafkaPublishStatus' in query
        assert 'SourceFilename' in query
        assert 'INGESTION_TIMESTAMP' in query

    @patch('fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job.bigquery.Client')
    def test_load_data_to_bigquery_task_missing_xcom_data(
        self,
        mock_bq_client,
        mock_module,
        mock_environment_config,
        mock_config
    ):
        """Test data loading fails when XCom data is missing"""
        from fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job import FraudRiskTruthDataDagBuilder

        builder = FraudRiskTruthDataDagBuilder(mock_environment_config)

        mock_ti = MagicMock()
        # Return None for staging_table_id, but provide a valid filename
        # The code calls basename(filename) before checking, so we need to provide a valid filename
        # but None staging_table_id to trigger the proper check
        mock_ti.xcom_pull.side_effect = lambda key, task_ids: {
            'staging_table_id': None,
            'file_name': 'test-file.csv',  # Valid filename to avoid TypeError in basename
            'env': 'uat'
        }.get(key)

        context = {
            'config': mock_config,
            'ti': mock_ti
        }

        with pytest.raises(AirflowFailException) as exc_info:
            builder._load_data_to_bigquery_task(**context)

        assert "Missing required data from validation task" in str(exc_info.value)

    @patch('fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job.run_bq_query')
    @patch('fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job.bigquery.Client')
    def test_load_data_to_bigquery_task_column_mapping(
        self,
        mock_bq_client,
        mock_run_bq_query,
        mock_module,
        mock_environment_config,
        mock_config
    ):
        """Test column mapping in data loading"""
        from fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job import FraudRiskTruthDataDagBuilder

        builder = FraudRiskTruthDataDagBuilder(mock_environment_config)

        mock_ti = MagicMock()
        mock_ti.xcom_pull.side_effect = lambda key, task_ids: {
            'staging_table_id': 'test-project.test-dataset.staging_table',
            'file_name': 'test-file.csv',
            'env': 'uat'
        }.get(key)

        mock_client = MagicMock()
        mock_bq_client.return_value = mock_client

        # Mock table schema with lowercase column names
        mock_table = MagicMock()
        mock_table.schema = [
            SchemaField('request_id', 'STRING'),
            SchemaField('event_tag', 'STRING')
        ]
        mock_table.num_rows = 50
        mock_client.get_table.return_value = mock_table

        context = {
            'config': mock_config,
            'ti': mock_ti
        }

        # Execute
        builder._load_data_to_bigquery_task(**context)

        # Verify column mapping in query
        query = mock_run_bq_query.call_args[0][0]
        assert '`request_id` as `RequestId`' in query
        assert '`event_tag` as `EventTag`' in query

    @patch('fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job.run_bq_query')
    @patch('fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job.bigquery.Client')
    def test_load_data_to_bigquery_task_error_handling(
        self,
        mock_bq_client,
        mock_run_bq_query,
        mock_module,
        mock_environment_config,
        mock_config
    ):
        """Test error handling in data loading"""
        from fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job import FraudRiskTruthDataDagBuilder

        builder = FraudRiskTruthDataDagBuilder(mock_environment_config)

        mock_ti = MagicMock()
        mock_ti.xcom_pull.side_effect = lambda key, task_ids: {
            'staging_table_id': 'test-project.test-dataset.staging_table',
            'file_name': 'test-file.csv',
            'env': 'uat'
        }.get(key)

        mock_client = MagicMock()
        mock_bq_client.return_value = mock_client
        mock_client.get_table.side_effect = Exception("Table not found")

        context = {
            'config': mock_config,
            'ti': mock_ti
        }

        with pytest.raises(AirflowFailException) as exc_info:
            builder._load_data_to_bigquery_task(**context)

        assert "Failed to load data" in str(exc_info.value)

    @patch('fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job.TriggerDagRunOperator')
    @patch('fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job.EmptyOperator')
    @patch('fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job.PythonOperator')
    @patch('fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job.DAG')
    def test_build_dag(
        self,
        mock_dag,
        mock_python_operator,
        mock_empty_operator,
        mock_trigger_dag_operator,
        mock_module,
        mock_environment_config,
        mock_config
    ):
        """Test DAG building"""
        from fraud_risk_truthdata_processing.fraud_risk_truthdata_data_loading_job import FraudRiskTruthDataDagBuilder

        builder = FraudRiskTruthDataDagBuilder(mock_environment_config)

        # Mock DAG instance with context manager support
        mock_dag_instance = MagicMock()
        mock_dag_instance.__enter__ = MagicMock(return_value=mock_dag_instance)
        mock_dag_instance.__exit__ = MagicMock(return_value=False)
        mock_dag.return_value = mock_dag_instance

        # Mock operators
        mock_start_task = MagicMock()
        mock_validation_task = MagicMock()
        mock_load_task = MagicMock()
        mock_kafka_task = MagicMock()
        mock_end_task = MagicMock()
        mock_empty_operator.side_effect = [mock_start_task, mock_end_task]
        mock_python_operator.side_effect = [mock_validation_task, mock_load_task]
        mock_trigger_dag_operator.return_value = mock_kafka_task

        # Execute
        dag = builder.build('test_dag_id', mock_config)

        # Assert
        assert dag == mock_dag_instance
        mock_dag.assert_called_once()
        assert mock_empty_operator.call_count == 2  # start and end tasks
        assert mock_python_operator.call_count == 2  # validation and load tasks
        assert mock_trigger_dag_operator.call_count == 1  # kafka trigger task
