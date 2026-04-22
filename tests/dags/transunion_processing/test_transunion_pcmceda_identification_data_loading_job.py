from unittest.mock import patch, MagicMock, Mock
import pytest
import sys
import os

# Add the dags directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'dags'))


@pytest.fixture
def mock_module():
    mock_constants = MagicMock()
    mock_constants.COLUMNS = 'columns'
    mock_constants.COMMA_SPACE = ', '

    # Mock the util module and its submodules
    mock_util = MagicMock()
    mock_util.constants = mock_constants

    # Create proper mocks for bq_utils functions
    mock_bq_utils = MagicMock()
    mock_bq_utils.run_bq_query = MagicMock()
    mock_bq_utils.get_table_columns = MagicMock()
    mock_bq_utils.table_exists = MagicMock()
    mock_util.bq_utils = mock_bq_utils

    # Create a mock BaseDagBuilder class that can be inherited from
    class MockBaseDagBuilder:
        def __init__(self, environment_config):
            self.environment_config = environment_config

        def prepare_default_args(self):
            return {}

    with patch.dict('sys.modules', {
        'dag_factory': MagicMock(),
        'dag_factory.abc': MagicMock(BaseDagBuilder=MockBaseDagBuilder),
        'dag_factory.terminus_dag_factory': MagicMock(),
        'dag_factory.environment_config': MagicMock(),
        'util': mock_util,
        'util.constants': mock_constants,
        'util.bq_utils': mock_util.bq_utils,
        'google.cloud.bigquery': MagicMock(),
        'google.cloud.bigquery.Client': MagicMock(),
        'airflow': MagicMock(),
        'airflow.operators.python': MagicMock(),
        'airflow.operators.empty': MagicMock(),
        'airflow.exceptions': MagicMock(),
    }):
        yield


@pytest.fixture
def mock_bq_client():
    # Mock the BigQuery client without importing the actual module
    mock_client = MagicMock()
    return mock_client


@pytest.fixture
def mock_environment_config():
    mock_config = MagicMock()
    mock_config.local_tz = None
    mock_config.gcp_config = {'processing_zone_project_id': 'test-project'}
    mock_config.deploy_env = 'test'
    return mock_config


def test_transform_and_load_final_table_success(mock_module, mock_environment_config):
    with patch('transunion_processing.transunion_pcmceda_identification_data_loading_job.get_table_columns') as mock_get_table_columns, \
         patch('transunion_processing.transunion_pcmceda_identification_data_loading_job.run_bq_query') as mock_run_bq_query:
        # Import the class after mocking
        from transunion_processing.transunion_pcmceda_identification_data_loading_job import TransUnionDagBuilder
        builder = TransUnionDagBuilder(mock_environment_config)

        config = {
            'field_mappings': {
                'base_fields': [
                    {'index': 0, 'target': 'RECORD_TYPE', 'type': 'STRING'},
                    {'index': 1, 'target': 'APP_ID', 'type': 'STRING'},
                    {'index': 2, 'target': 'SCORE', 'type': 'FLOAT'},
                    {'index': 3, 'target': 'CREATED_DATE', 'type': 'DATETIME'},
                ],
                'question_fields': [
                    {'index': 4, 'target': 'QUESTION_1', 'type': 'STRING'},
                ],
                'metadata_fields': [
                    {'target': 'SOURCE_FILENAME', 'type': 'STRING', 'value': 'file_name'},
                    {'target': 'REC_LOAD_TIMESTAMP', 'type': 'TIMESTAMP', 'value': 'CURRENT_TIMESTAMP()'},
                ]
            },
            'filtering': {
                'record_type_field_index': 0,
                'detail_record_value': 'D'
            },
            'timestamp_format': '%Y%m%d%H%M%S'
        }

        # Configure the mocks
        mock_get_table_columns.return_value = {
            'columns': 'field_0, field_1, field_2, field_3, field_4'
        }
        mock_run_bq_query.return_value = [{'count': 100}]

        # Test the method
        result = builder.transform_and_load_final_table(
            client=Mock(),
            staging_table_id='project.dataset.staging_table',
            final_table_id='project.dataset.final_table',
            file_name='test_file.csv',
            config=config
        )

        # Assertions
        assert result is None  # Method should return None on success
        mock_get_table_columns.assert_called_once()
        mock_run_bq_query.assert_called_once()

        # Verify the SQL query
        call_args = mock_run_bq_query.call_args
        query = call_args[0][0]  # First positional argument

        # Check that the query contains expected elements
        assert 'INSERT INTO `project.dataset.final_table`' in query
        assert 'FROM `project.dataset.staging_table`' in query
        assert "WHERE `field_0` = 'D'" in query
        assert 'CAST(`field_0` AS STRING) as RECORD_TYPE' in query
        assert 'CAST(`field_2` AS FLOAT64) as SCORE' in query
        assert 'PARSE_DATETIME' in query
        assert 'test_file.csv' in query
        assert 'CURRENT_TIMESTAMP()' in query


def test_get_trailer_record_count_success(mock_module, mock_environment_config):
    with patch('transunion_processing.transunion_pcmceda_identification_data_loading_job.get_table_columns') as mock_get_table_columns, \
         patch('transunion_processing.transunion_pcmceda_identification_data_loading_job.run_bq_query') as mock_run_bq_query:
        from transunion_processing.transunion_pcmceda_identification_data_loading_job import TransUnionDagBuilder
        builder = TransUnionDagBuilder(mock_environment_config)

        # Configure the mocks
        mock_get_table_columns.return_value = {
            'columns': 'field_0, field_1, field_2, field_3'
        }

        mock_row = MagicMock()
        mock_row.expected_count = '100'
        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([mock_row])
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = mock_result
        mock_run_bq_query.return_value = mock_query_job

        mock_client = Mock()
        staging_table_id = 'project.dataset.staging_table'

        result = builder.get_trailer_record_count(mock_client, staging_table_id, 'T', 1, 0)

        assert result == 100
        mock_run_bq_query.assert_called_once()
        query = mock_run_bq_query.call_args[0][0]
        assert 'SELECT `field_1` as expected_count' in query
        assert 'FROM `project.dataset.staging_table`' in query
        assert "WHERE `field_0` = 'T'" in query
        assert 'LIMIT 1' in query


def test_get_trailer_record_count_not_found(mock_module, mock_environment_config):
    with patch('transunion_processing.transunion_pcmceda_identification_data_loading_job.get_table_columns') as mock_get_table_columns, \
         patch('transunion_processing.transunion_pcmceda_identification_data_loading_job.run_bq_query') as mock_run_bq_query:
        from transunion_processing.transunion_pcmceda_identification_data_loading_job import TransUnionDagBuilder
        builder = TransUnionDagBuilder(mock_environment_config)

        # Configure the mocks
        mock_get_table_columns.return_value = {
            'columns': 'field_0, field_1, field_2'
        }

        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([])
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = mock_result
        mock_run_bq_query.return_value = mock_query_job

        result = builder.get_trailer_record_count(Mock(), 'staging_table', 'T', 1, 0)

        assert result is None
        mock_run_bq_query.assert_called_once()


def test_validate_data_loaded_success(mock_bq_client, mock_module, mock_environment_config):
    with patch('transunion_processing.transunion_pcmceda_identification_data_loading_job.bigquery.Client', mock_bq_client):
        from transunion_processing.transunion_pcmceda_identification_data_loading_job import TransUnionDagBuilder
        builder = TransUnionDagBuilder(mock_environment_config)

        config = {
            'filtering': {
                'trailer_record_value': 'T',
                'trailer_count_field_index': 1,
                'record_type_field_index': 0
            }
        }

        # Mock the get_trailer_record_count method
        with patch.object(builder, 'get_trailer_record_count', return_value=100):
            with patch.object(builder, 'get_staging_table_id', return_value='project.dataset.staging_table'):
                with patch('transunion_processing.transunion_pcmceda_identification_data_loading_job.logger') as mock_logger:
                    mock_ti = MagicMock()
                    mock_ti.xcom_pull.return_value = 100

                    context = {'ti': mock_ti, 'config': config}

                    builder.validate_data_loaded(**context)

        mock_logger.info.assert_any_call('Validation successful: Inserted 100 rows from this file, matching trailer record count of 100')


def test_validate_data_loaded_count_mismatch(mock_bq_client, mock_module, mock_environment_config):
    with patch('transunion_processing.transunion_pcmceda_identification_data_loading_job.bigquery.Client', mock_bq_client):
        from transunion_processing.transunion_pcmceda_identification_data_loading_job import TransUnionDagBuilder
        builder = TransUnionDagBuilder(mock_environment_config)

        config = {
            'filtering': {
                'trailer_record_value': 'T',
                'trailer_count_field_index': 1,
                'record_type_field_index': 0
            }
        }

        # Mock the get_trailer_record_count method
        with patch.object(builder, 'get_trailer_record_count', return_value=100):
            with patch.object(builder, 'get_staging_table_id', return_value='project.dataset.staging_table'):
                mock_ti = MagicMock()
                mock_ti.xcom_pull.return_value = 95

                context = {'ti': mock_ti, 'config': config}

                with pytest.raises(Exception) as exc_info:
                    builder.validate_data_loaded(**context)

        assert "Row count mismatch" in str(exc_info.value)
        assert "Inserted 95 rows" in str(exc_info.value)
        assert "trailer record indicates 100 records" in str(exc_info.value)


def test_validate_data_loaded_no_trailer_found(mock_bq_client, mock_module, mock_environment_config):
    with patch('transunion_processing.transunion_pcmceda_identification_data_loading_job.bigquery.Client', mock_bq_client):
        from transunion_processing.transunion_pcmceda_identification_data_loading_job import TransUnionDagBuilder
        builder = TransUnionDagBuilder(mock_environment_config)

        config = {
            'filtering': {
                'trailer_record_value': 'T',
                'trailer_count_field_index': 1,
                'record_type_field_index': 0
            }
        }

        # Mock the get_trailer_record_count method
        with patch.object(builder, 'get_trailer_record_count', return_value=None):
            with patch.object(builder, 'get_staging_table_id', return_value='project.dataset.staging_table'):
                mock_ti = MagicMock()
                mock_ti.xcom_pull.return_value = 50

                context = {'ti': mock_ti, 'config': config}

                with pytest.raises(Exception) as exc_info:
                    builder.validate_data_loaded(**context)

        assert "Trailer record not found or invalid" in str(exc_info.value)
        assert "Cannot validate data integrity" in str(exc_info.value)
        assert "Inserted 50 rows" in str(exc_info.value)
