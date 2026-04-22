from unittest.mock import patch, MagicMock, Mock
import pytest
import sys
import os
from airflow.exceptions import AirflowFailException
from datetime import datetime
import pandas as pd

# Add the dags directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'dags'))


@pytest.fixture
def mock_module():
    """Mock module dependencies"""
    mock_constants = MagicMock()
    mock_constants.CONFIG = 'config'
    mock_constants.DAG = 'dag'
    mock_constants.QUERY = 'query'
    mock_constants.DESCRIPTION = 'description'
    mock_constants.TAGS = 'tags'
    mock_constants.DEFAULT_ARGS = 'default_args'
    mock_constants.START_TASK_ID = 'start'
    mock_constants.END_TASK_ID = 'end'
    mock_constants.TASK_ID = 'task_id'
    mock_constants.DAG_ID = 'dag_id'
    mock_constants.KAFKA_WRITER_TASK_ID = 'kafka_writer_task_id'
    mock_constants.KAFKA_TRIGGER_DAG_ID = 'kafka_trigger_dag_id'
    mock_constants.BUCKET = 'bucket'
    mock_constants.FOLDER_NAME = 'folder_name'
    mock_constants.FILE_NAME = 'file_name'
    mock_constants.WAIT_FOR_COMPLETION = 'wait_for_completion'
    mock_constants.POKE_INTERVAL = 'poke_interval'

    # Mock the util module and its submodules
    mock_util = MagicMock()
    mock_util.constants = mock_constants

    # Create proper mocks for bq_utils functions
    mock_bq_utils = MagicMock()
    mock_bq_utils.run_bq_query = MagicMock()
    mock_util.bq_utils = mock_bq_utils

    # Create proper mocks for miscutils
    mock_miscutils = MagicMock()
    mock_miscutils.read_file_env = MagicMock()
    mock_util.miscutils = mock_miscutils

    # Create proper mocks for gcs_utils
    mock_gcs_utils = MagicMock()
    mock_gcs_utils.cleanup_gcs_folder = MagicMock()
    mock_util.gcs_utils = mock_gcs_utils

    # Create a mock BaseDagBuilder class that can be inherited from
    class MockBaseDagBuilder:
        def __init__(self, environment_config):
            self.environment_config = environment_config

        def prepare_default_args(self, default_args):
            return default_args

    # Mock airflow.settings with DAGS_FOLDER attribute
    mock_airflow_settings = MagicMock()
    mock_airflow_settings.DAGS_FOLDER = '/test/dags'

    with patch.dict('sys.modules', {
        'util': mock_util,
        'util.constants': mock_constants,
        'util.bq_utils': mock_util.bq_utils,
        'util.miscutils': mock_miscutils,
        'util.gcs_utils': mock_gcs_utils,
        'airflow': MagicMock(),
        'airflow.operators.python': MagicMock(),
        'airflow.operators.empty': MagicMock(),
        'airflow.operators.trigger_dagrun': MagicMock(),
        'airflow.utils.trigger_rule': MagicMock(),
        'airflow.exceptions': MagicMock(AirflowFailException=AirflowFailException),
        'airflow.settings': mock_airflow_settings,
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
        'dag': {
            'description': 'Fraud Risk Truth Data Kafka processing from BigQuery',
            'schedule': None,
            'max_active_runs': 1,
            'is_paused_upon_creation': True,
            'tags': ['test'],
            'render_template_as_native_obj': True,
            'task_id': {
                'kafka_writer_task_id': 'trigger_kafka_writer_dag'
            },
            'dag_id': {
                'kafka_trigger_dag_id': 'fraud_risk_truthdata_kafka_writer'
            },
            'query': {
                'fraud_risk_truthdata_count': 'fraud_risk_truthdata_count.sql',
                'fraud_risk_truthdata': 'fraud_risk_truthdata.sql',
                'fraud_risk_truthdata_update': 'fraud_risk_truthdata_update.sql'
            },
            'wait_for_completion': True,
            'poke_interval': 30,
            'bucket': 'pcb-{env}-staging-extract',
            'folder_name': 'fraud_risk_truthdata',
            'file_name': 'fraud-risk-truthdata-*.parquet'
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

    context = {
        'config': mock_config,
        'dag_run': mock_dag_run,
        'ti': mock_ti
    }
    return context


class TestFraudRiskTruthDataKafkaProcessorDagBuilder:
    """Test cases for FraudRiskTruthDataKafkaProcessorDagBuilder class"""

    def test_initialization(self, mock_module, mock_environment_config):
        """Test DAG builder initialization"""
        from fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka import FraudRiskTruthDataKafkaProcessorDagBuilder

        builder = FraudRiskTruthDataKafkaProcessorDagBuilder(mock_environment_config)

        assert builder.local_tz == mock_environment_config.local_tz
        assert builder.gcp_config == mock_environment_config.gcp_config
        assert builder.deploy_env == mock_environment_config.deploy_env

    @patch('fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka.read_file_env')
    @patch('fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka.run_bq_query')
    def test_export_data_to_parquet_from_bigquery_success(
        self,
        mock_run_bq_query,
        mock_read_file_env,
        mock_module,
        mock_environment_config,
        mock_config,
        mock_dag_context
    ):
        """Test successful export of data to parquet from BigQuery"""
        from fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka import FraudRiskTruthDataKafkaProcessorDagBuilder

        builder = FraudRiskTruthDataKafkaProcessorDagBuilder(mock_environment_config)

        # Setup mocks
        mock_sql = "EXPORT DATA OPTIONS (uri = 'gs://...', format = 'PARQUET') AS SELECT * FROM ..."
        mock_read_file_env.return_value = mock_sql

        mock_query_result = MagicMock()
        mock_dataframe = pd.DataFrame({'col1': [1, 2, 3]})
        mock_query_result.to_dataframe.return_value = mock_dataframe
        mock_run_bq_query.return_value = mock_query_result

        # Execute
        builder.export_data_to_parquet_from_bigquery(**mock_dag_context)

        # Assert
        mock_read_file_env.assert_called_once()
        mock_run_bq_query.assert_called_once_with(mock_sql)
        mock_query_result.to_dataframe.assert_called_once()

    @patch('fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka.read_file_env')
    @patch('fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka.run_bq_query')
    def test_export_data_to_parquet_from_bigquery_error(
        self,
        mock_run_bq_query,
        mock_read_file_env,
        mock_module,
        mock_environment_config,
        mock_config,
        mock_dag_context
    ):
        """Test error handling when export to parquet fails"""
        from fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka import FraudRiskTruthDataKafkaProcessorDagBuilder

        builder = FraudRiskTruthDataKafkaProcessorDagBuilder(mock_environment_config)

        # Setup mocks
        mock_sql = "EXPORT DATA OPTIONS (uri = 'gs://...', format = 'PARQUET') AS SELECT * FROM ..."
        mock_read_file_env.return_value = mock_sql
        mock_run_bq_query.side_effect = Exception("BigQuery export failed")

        # Execute and assert
        with pytest.raises(Exception) as exc_info:
            builder.export_data_to_parquet_from_bigquery(**mock_dag_context)

        assert "BigQuery export failed" in str(exc_info.value)

    @patch('fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka.cleanup_gcs_folder')
    def test_clear_extracted_files_success(
        self,
        mock_cleanup_gcs_folder,
        mock_module,
        mock_environment_config,
        mock_config,
        mock_dag_context
    ):
        """Test successful clearing of extracted files from GCS"""
        from fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka import FraudRiskTruthDataKafkaProcessorDagBuilder

        builder = FraudRiskTruthDataKafkaProcessorDagBuilder(mock_environment_config)
        mock_cleanup_gcs_folder.return_value = None

        # Execute
        builder.clear_extracted_files(**mock_dag_context)

        # Assert - bucket has {env} replaced with deploy_env 'test'
        mock_cleanup_gcs_folder.assert_called_once_with(
            bucket_name='pcb-test-staging-extract',
            folder_prefix='fraud_risk_truthdata/'
        )

    @patch('fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka.cleanup_gcs_folder')
    def test_clear_extracted_files_failure(
        self,
        mock_cleanup_gcs_folder,
        mock_module,
        mock_environment_config,
        mock_config,
        mock_dag_context
    ):
        """Test error handling when clear extracted files fails"""
        from fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka import FraudRiskTruthDataKafkaProcessorDagBuilder

        builder = FraudRiskTruthDataKafkaProcessorDagBuilder(mock_environment_config)
        mock_cleanup_gcs_folder.side_effect = Exception("GCS cleanup failed")

        # Execute and assert
        with pytest.raises(AirflowFailException) as exc_info:
            builder.clear_extracted_files(**mock_dag_context)

        assert "Failed to clear extracted files" in str(exc_info.value)
        assert "GCS cleanup failed" in str(exc_info.value)

    @patch('fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka.read_file_env')
    @patch('fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka.run_bq_query')
    def test_update_kafka_publish_status_success(
        self,
        mock_run_bq_query,
        mock_read_file_env,
        mock_module,
        mock_environment_config,
        mock_config,
        mock_dag_context
    ):
        """Test successful update of Kafka publish status"""
        from fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka import FraudRiskTruthDataKafkaProcessorDagBuilder

        builder = FraudRiskTruthDataKafkaProcessorDagBuilder(mock_environment_config)

        # Setup mocks
        mock_sql = "UPDATE ... SET KafkaPublishStatus = 'COMPLETED' WHERE ..."
        mock_read_file_env.return_value = mock_sql
        mock_run_bq_query.return_value = None

        # Execute
        result = builder.update_kafka_publish_status(**mock_dag_context)

        # Assert
        assert "SUCCESS" in result
        assert "PENDING to COMPLETED" in result
        mock_read_file_env.assert_called_once()
        mock_run_bq_query.assert_called_once_with(mock_sql)

    @patch('fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka.read_file_env')
    @patch('fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka.run_bq_query')
    def test_update_kafka_publish_status_failure(
        self,
        mock_run_bq_query,
        mock_read_file_env,
        mock_module,
        mock_environment_config,
        mock_config,
        mock_dag_context
    ):
        """Test error handling when update Kafka publish status fails"""
        from fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka import FraudRiskTruthDataKafkaProcessorDagBuilder

        builder = FraudRiskTruthDataKafkaProcessorDagBuilder(mock_environment_config)

        # Setup mocks
        mock_sql = "UPDATE ... SET KafkaPublishStatus = 'COMPLETED' WHERE ..."
        mock_read_file_env.return_value = mock_sql
        mock_run_bq_query.side_effect = Exception("Update failed")

        # Execute and assert
        with pytest.raises(AirflowFailException) as exc_info:
            builder.update_kafka_publish_status(**mock_dag_context)

        assert "Failed to update KafkaPublishStatus" in str(exc_info.value)
        assert "Update failed" in str(exc_info.value)

    @patch('fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka.read_file_env')
    @patch('fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka.settings')
    def test_get_truthdata_sql(
        self,
        mock_settings,
        mock_read_file_env,
        mock_module,
        mock_environment_config
    ):
        """Test reading and formatting SQL file with environment substitution"""
        from fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka import FraudRiskTruthDataKafkaProcessorDagBuilder

        builder = FraudRiskTruthDataKafkaProcessorDagBuilder(mock_environment_config)

        # Setup mocks
        mock_settings.DAGS_FOLDER = '/test/dags'
        mock_sql = "SELECT * FROM `pcb-{env}-landing.dataset.table`"
        mock_read_file_env.return_value = mock_sql

        config = {
            'dag': {
                'query': {
                    'fraud_risk_truthdata': 'fraud_risk_truthdata.sql'
                }
            }
        }

        # Execute
        result = builder.get_truthdata_sql('fraud_risk_truthdata.sql', config)

        # Assert
        assert result == mock_sql
        expected_file_path = f'{mock_settings.DAGS_FOLDER}/fraud_risk_truthdata_processing/sql/fraud_risk_truthdata.sql'
        mock_read_file_env.assert_called_once_with(expected_file_path, 'test')

    @patch('fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka.read_file_env')
    @patch('fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka.run_bq_query')
    def test_evaluate_truthdata_count_with_records(
        self,
        mock_run_bq_query,
        mock_read_file_env,
        mock_module,
        mock_environment_config,
        mock_config,
        mock_dag_context
    ):
        """Test evaluate truthdata count when records exist (count > 0)"""
        from fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka import FraudRiskTruthDataKafkaProcessorDagBuilder

        builder = FraudRiskTruthDataKafkaProcessorDagBuilder(mock_environment_config)

        # Setup mocks
        mock_sql = "SELECT count(RequestId) AS truthdata_count FROM ..."
        mock_read_file_env.return_value = mock_sql

        mock_query_result = MagicMock()
        mock_dataframe = pd.DataFrame({'truthdata_count': [5]})
        mock_query_result.to_dataframe.return_value = mock_dataframe
        mock_run_bq_query.return_value = mock_query_result

        # Execute
        result = builder.evaluate_truthdata_count(**mock_dag_context)

        # Assert
        assert result == 'export_data_to_parquet_from_bigquery'
        mock_read_file_env.assert_called_once()
        mock_run_bq_query.assert_called_once_with(mock_sql)

    @patch('fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka.read_file_env')
    @patch('fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka.run_bq_query')
    def test_evaluate_truthdata_count_no_records(
        self,
        mock_run_bq_query,
        mock_read_file_env,
        mock_module,
        mock_environment_config,
        mock_config,
        mock_dag_context
    ):
        """Test evaluate truthdata count when no records exist (count == 0)"""
        from fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka import FraudRiskTruthDataKafkaProcessorDagBuilder

        builder = FraudRiskTruthDataKafkaProcessorDagBuilder(mock_environment_config)

        # Setup mocks
        mock_sql = "SELECT count(RequestId) AS truthdata_count FROM ..."
        mock_read_file_env.return_value = mock_sql

        mock_query_result = MagicMock()
        mock_dataframe = pd.DataFrame({'truthdata_count': [0]})
        mock_query_result.to_dataframe.return_value = mock_dataframe
        mock_run_bq_query.return_value = mock_query_result

        # Execute
        result = builder.evaluate_truthdata_count(**mock_dag_context)

        # Assert
        assert result == 'no_records_task'
        mock_read_file_env.assert_called_once()
        mock_run_bq_query.assert_called_once_with(mock_sql)

    @patch('fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka.read_file_env')
    @patch('fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka.run_bq_query')
    def test_evaluate_truthdata_count_error(
        self,
        mock_run_bq_query,
        mock_read_file_env,
        mock_module,
        mock_environment_config,
        mock_config,
        mock_dag_context
    ):
        """Test error handling when evaluate truthdata count fails"""
        from fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka import FraudRiskTruthDataKafkaProcessorDagBuilder

        builder = FraudRiskTruthDataKafkaProcessorDagBuilder(mock_environment_config)

        # Setup mocks
        mock_sql = "SELECT count(RequestId) AS truthdata_count FROM ..."
        mock_read_file_env.return_value = mock_sql
        mock_run_bq_query.side_effect = Exception("Query failed")

        # Execute and assert
        with pytest.raises(Exception) as exc_info:
            builder.evaluate_truthdata_count(**mock_dag_context)

        assert "Query failed" in str(exc_info.value)

    @patch('fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka.EmptyOperator')
    @patch('fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka.PythonOperator')
    @patch('fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka.BranchPythonOperator')
    @patch('fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka.TriggerDagRunOperator')
    @patch('fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka.DAG')
    def test_build_dag(
        self,
        mock_dag,
        mock_trigger_dag_run_operator,
        mock_branch_python_operator,
        mock_python_operator,
        mock_empty_operator,
        mock_module,
        mock_environment_config,
        mock_config
    ):
        """Test DAG building with proper task dependencies"""
        from fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka import FraudRiskTruthDataKafkaProcessorDagBuilder
        from airflow.utils.trigger_rule import TriggerRule

        builder = FraudRiskTruthDataKafkaProcessorDagBuilder(mock_environment_config)

        # Mock DAG instance with context manager support
        mock_dag_instance = MagicMock()
        mock_dag_instance.__enter__ = MagicMock(return_value=mock_dag_instance)
        mock_dag_instance.__exit__ = MagicMock(return_value=False)
        mock_dag.return_value = mock_dag_instance

        # Mock operators
        mock_start_task = MagicMock()
        mock_clear_task = MagicMock()
        mock_evaluate_task = MagicMock()
        mock_export_task = MagicMock()
        mock_no_records_task = MagicMock()
        mock_kafka_writer_task = MagicMock()
        mock_update_status_task = MagicMock()
        mock_end_task = MagicMock()

        mock_empty_operator.side_effect = [mock_start_task, mock_end_task]
        mock_branch_python_operator.return_value = mock_evaluate_task
        mock_python_operator.side_effect = [
            mock_clear_task,      # clear_previous_extracted_files
            mock_export_task,     # export_data_to_parquet_from_bigquery
            mock_no_records_task,   # no_records_task
            mock_update_status_task  # update_kafka_publish_status
        ]
        mock_trigger_dag_run_operator.return_value = mock_kafka_writer_task

        # Execute
        dag = builder.build('test_dag_id', mock_config)

        # Assert
        assert dag == mock_dag_instance
        mock_dag.assert_called_once()
        assert mock_empty_operator.call_count == 2  # start and end tasks
        assert mock_branch_python_operator.call_count == 1  # evaluate task
        assert mock_python_operator.call_count == 4  # clear, export, no_records, update_status tasks
        assert mock_trigger_dag_run_operator.call_count == 1  # kafka writer task

        # Verify task dependencies were set up correctly
        # The >> operator should have been called to set up dependencies
        # We can't directly verify this, but we can verify the operators were created

    @patch('fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka.read_file_env')
    @patch('fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka.run_bq_query')
    def test_evaluate_truthdata_count_multiple_records(
        self,
        mock_run_bq_query,
        mock_read_file_env,
        mock_module,
        mock_environment_config,
        mock_config,
        mock_dag_context
    ):
        """Test evaluate truthdata count with multiple records"""
        from fraud_risk_truthdata_processing.fraud_risk_truth_data_processing_to_kafka import FraudRiskTruthDataKafkaProcessorDagBuilder

        builder = FraudRiskTruthDataKafkaProcessorDagBuilder(mock_environment_config)

        # Setup mocks
        mock_sql = "SELECT count(RequestId) AS truthdata_count FROM ..."
        mock_read_file_env.return_value = mock_sql

        mock_query_result = MagicMock()
        mock_dataframe = pd.DataFrame({'truthdata_count': [100]})
        mock_query_result.to_dataframe.return_value = mock_dataframe
        mock_run_bq_query.return_value = mock_query_result

        # Execute
        result = builder.evaluate_truthdata_count(**mock_dag_context)

        # Assert
        assert result == 'export_data_to_parquet_from_bigquery'
        assert mock_dataframe['truthdata_count'].iloc[0] == 100
