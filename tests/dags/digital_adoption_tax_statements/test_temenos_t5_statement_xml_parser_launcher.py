import pytest
from unittest.mock import patch, MagicMock, mock_open
import pandas as pd
import xml.etree.ElementTree as et
from airflow.models.dag import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os
import tempfile
import xmlschema
from google.cloud import bigquery, storage

from digital_adoption_tax_statements.temenos_t5.t5_statement_xml_parser_launcher import (
    dag, fetch_xml_from_gcs, load_xsd, validate_xml, xml_validation_task, list_xml_files,
    create_external_table_header, generate_and_execute_insert_query, ingest_t5_header_data,
    validate_t5_slip_data, validate_counts_and_amounts, validate_summary_and_slip_bn,
    get_xsd_path, extract_tx_yr_from_xml
)


@pytest.fixture
def mock_read_variable_or_file():
    """Fixture for mocking read_variable_or_file function."""
    with patch('digital_adoption_tax_statements.temenos_t5.t5_statement_xml_parser_launcher.read_variable_or_file') as mock_func:
        mock_func.return_value = {
            'deployment_environment_name': 'test',
            'bq_query_location': 'US'
        }
        yield mock_func


@pytest.fixture
def mock_bigquery_client():
    """Fixture for mocking BigQuery client."""
    with patch('digital_adoption_tax_statements.temenos_t5.t5_statement_xml_parser_launcher.bigquery.Client') as mock_client_class:
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        yield mock_client_class


@pytest.fixture
def mock_storage_client():
    """Fixture for mocking Google Cloud Storage client."""
    with patch('digital_adoption_tax_statements.temenos_t5.t5_statement_xml_parser_launcher.storage.Client') as mock_client_class:
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()

        mock_client_class.return_value = mock_client
        mock_client.get_bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        yield {
            'client': mock_client,
            'bucket': mock_bucket,
            'blob': mock_blob
        }


@pytest.fixture
def mock_xmlschema():
    """Fixture for mocking xmlschema.XMLSchema."""
    with patch('digital_adoption_tax_statements.temenos_t5.t5_statement_xml_parser_launcher.xmlschema.XMLSchema') as mock_schema_class:
        mock_schema = MagicMock()
        mock_schema_class.return_value = mock_schema
        yield mock_schema


@pytest.fixture
def mock_pandas_dataframe():
    """Fixture for mocking pandas DataFrame."""
    with patch('digital_adoption_tax_statements.temenos_t5.t5_statement_xml_parser_launcher.pd.DataFrame') as mock_df_class:
        mock_dataframe = MagicMock()
        mock_df_class.return_value = mock_dataframe
        yield mock_dataframe


@pytest.fixture
def mock_create_external_table():
    """Fixture for mocking create_external_table function."""
    with patch('digital_adoption_tax_statements.temenos_t5.t5_statement_xml_parser_launcher.create_external_table') as mock_func:
        yield mock_func


@pytest.fixture
def mock_generate_and_execute_insert_query():
    """Fixture for mocking generate_and_execute_insert_query function."""
    with patch('digital_adoption_tax_statements.temenos_t5.t5_statement_xml_parser_launcher.generate_and_execute_insert_query') as mock_func:
        yield mock_func


@pytest.fixture
def mock_xml_validation_functions():
    """Fixture for mocking XML validation related functions."""
    with patch('digital_adoption_tax_statements.temenos_t5.t5_statement_xml_parser_launcher.fetch_xml_from_gcs') as mock_fetch, \
         patch('digital_adoption_tax_statements.temenos_t5.t5_statement_xml_parser_launcher.load_xsd') as mock_load, \
         patch('digital_adoption_tax_statements.temenos_t5.t5_statement_xml_parser_launcher.validate_xml') as mock_validate:

        yield {
            'fetch_xml': mock_fetch,
            'load_xsd': mock_load,
            'validate_xml': mock_validate
        }


@pytest.fixture
def mock_run_bq_query():
    """Fixture for mocking run_bq_query function."""
    with patch('digital_adoption_tax_statements.temenos_t5.t5_statement_xml_parser_launcher.run_bq_query') as mock_func:
        yield mock_func


@pytest.fixture
def sample_xml_content():
    """Fixture providing sample XML content for testing."""
    return '''<?xml version="1.0" encoding="UTF-8"?>
        <root>
            <T5Slip>
                <field1>value1</field1>
                <field2>value2</field2>
            </T5Slip>
            <T619>
                <header1>hvalue1</header1>
            </T619>
            <T5Summary>
                <tx_yr>2025</tx_yr>
                <summary1>svalue1</summary1>
            </T5Summary>
        </root>'''


@pytest.fixture
def sample_xml_content_2024():
    """Fixture providing sample XML content for 2024 tax year."""
    return '''<?xml version="1.0" encoding="UTF-8"?>
        <Submission>
            <T5Slip>
                <RCPNT_NM>
                    <snm>TestLastName</snm>
                    <gvn_nm>TestFirstName</gvn_nm>
                </RCPNT_NM>
            </T5Slip>
            <T5Summary>
                <tx_yr>2024</tx_yr>
            </T5Summary>
        </Submission>'''


@pytest.fixture
def mock_bigquery_table():
    """Fixture for mocking BigQuery table schema."""
    mock_table = MagicMock()
    mock_field1 = MagicMock()
    mock_field1.name = 'column1'
    mock_field2 = MagicMock()
    mock_field2.name = 'column2'
    mock_table.schema = [mock_field1, mock_field2]
    return mock_table


class TestTemenosT5StatementXMLParserLauncher:
    """Test suite for T5 Statement XML Parser Launcher DAG."""

    def test_dag_structure(self, mock_read_variable_or_file):
        """Test DAG structure and configuration."""
        # Check DAG ID
        assert dag.dag_id == 't5_statement_launcher'

        # Check default args
        assert dag.default_args['owner'] == 'team-digital-adoption-alerts'
        assert dag.default_args['capability'] == 'account-management'
        assert dag.default_args['severity'] == 'P2'
        assert dag.default_args['max_active_runs'] == 1
        assert dag.default_args['retries'] == 3

        # Check schedule
        assert dag.schedule_interval is None

        # Check is_paused_upon_creation
        assert dag.is_paused_upon_creation is True

    def test_dag_tasks_and_dependencies(self, mock_read_variable_or_file):
        """Test that all expected tasks are present in the DAG."""
        expected_task_ids = [
            'start',
            'gcs_to_gcs',
            'validate_xml_with_xsd',
            'transform_xml_to_CSV',
            'create_external_table_header',
            'create_external_table_trailer',
            'create_external_table_t5slip',
            'ingest_t5_header_data',
            'ingest_t5_slip_data',
            'ingest_t5_trailer_data',
            't5_tax_slip_transformation',
            'validate_t5_slip_data',
            'validate_counts_and_amounts',
            'validate_summary_and_slip_bn',
            'trigger_t5_generate_xml_dag',
            'trigger_temenos_t5_kafka_writer_dag',
            'insert_t5_tax_header',
            'insert_t5_tax_trailer',
            'insert_t5_tax_slip',
            'end'
        ]

        actual_task_ids = set(dag.task_dict.keys())
        assert set(expected_task_ids).issubset(actual_task_ids)

        # Check task dependencies
        for task, downstream_task in zip(expected_task_ids, expected_task_ids[1:]):
            current_task = dag.get_task(task)
            assert current_task.downstream_list[0].task_id == downstream_task

    def test_fetch_xml_from_gcs(self, mock_storage_client, mock_read_variable_or_file):
        """Test fetching XML content from Google Cloud Storage."""
        # Arrange
        mock_storage_client['blob'].download_as_text.return_value = '<test>xml content</test>'

        # Act
        result = fetch_xml_from_gcs()

        # Assert
        assert result == '<test>xml content</test>'

    def test_load_xsd(self, mock_xmlschema):
        """Test loading XSD schema."""
        # Act
        result = load_xsd('test.xsd')

        # Assert
        assert result == mock_xmlschema

    def test_validate_xml_valid(self):
        """Test XML validation with valid XML."""
        # Arrange
        mock_schema = MagicMock()
        mock_schema.is_valid.return_value = True
        xml_content = '<test>valid xml</test>'

        # Act
        validate_xml(xml_content, mock_schema)

        # Assert
        mock_schema.is_valid.assert_called_once_with(xml_content)

    def test_validate_xml_invalid(self):
        """Test XML validation with invalid XML."""
        # Arrange
        mock_schema = MagicMock()
        mock_schema.is_valid.return_value = False
        mock_error = MagicMock()
        mock_error.__str__.return_value = "Reason: Invalid element 'test' Schema component: element"
        mock_schema.iter_errors.return_value = [mock_error]
        xml_content = '<invalid>xml</invalid>'

        # Act & Assert
        with pytest.raises(ValueError, match="XML Validation Errors: Invalid element 'test'"):
            validate_xml(xml_content, mock_schema)

    def test_get_xsd_path_2024(self):
        """Test get_xsd_path returns correct path for 2024."""
        # Act
        result = get_xsd_path('2024')

        # Assert
        assert 't5_xmlschm_2024' in result
        assert result.endswith('T619_T5.xsd')

    def test_get_xsd_path_2025(self):
        """Test get_xsd_path returns correct path for 2025."""
        # Act
        result = get_xsd_path('2025')

        # Assert
        assert 't5_xmlschm_2025' in result
        assert result.endswith('T619_T5.xsd')

    def test_get_xsd_path_unsupported_year(self):
        """Test get_xsd_path raises error for unsupported year."""
        # Act & Assert
        with pytest.raises(ValueError, match="Unsupported tax year: 2020"):
            get_xsd_path('2020')

    def test_extract_tx_yr_from_xml_valid(self):
        """Test extract_tx_yr_from_xml with valid XML."""
        # Arrange
        xml_content = '''<?xml version="1.0"?>
            <Submission>
                <T5Summary>
                    <tx_yr>2025</tx_yr>
                </T5Summary>
            </Submission>'''

        # Act
        result = extract_tx_yr_from_xml(xml_content)

        # Assert
        assert result == '2025'

    def test_extract_tx_yr_from_xml_2024(self):
        """Test extract_tx_yr_from_xml with 2024 tax year."""
        # Arrange
        xml_content = '''<?xml version="1.0"?>
            <Submission>
                <T5Summary>
                    <tx_yr>2024</tx_yr>
                </T5Summary>
            </Submission>'''

        # Act
        result = extract_tx_yr_from_xml(xml_content)

        # Assert
        assert result == '2024'

    def test_extract_tx_yr_from_xml_with_whitespace(self):
        """Test extract_tx_yr_from_xml strips whitespace."""
        # Arrange
        xml_content = '''<?xml version="1.0"?>
            <Submission>
                <T5Summary>
                    <tx_yr>  2025  </tx_yr>
                </T5Summary>
            </Submission>'''

        # Act
        result = extract_tx_yr_from_xml(xml_content)

        # Assert
        assert result == '2025'

    def test_extract_tx_yr_from_xml_missing_element(self):
        """Test extract_tx_yr_from_xml raises error when tx_yr is missing."""
        # Arrange
        xml_content = '''<?xml version="1.0"?>
            <Submission>
                <T5Summary>
                    <other_field>value</other_field>
                </T5Summary>
            </Submission>'''

        # Act & Assert
        with pytest.raises(ValueError, match="tx_yr element not found"):
            extract_tx_yr_from_xml(xml_content)

    def test_extract_tx_yr_from_xml_invalid_xml(self):
        """Test extract_tx_yr_from_xml raises error for invalid XML."""
        # Arrange
        xml_content = 'not valid xml content'

        # Act & Assert
        with pytest.raises(ValueError, match="Failed to parse XML"):
            extract_tx_yr_from_xml(xml_content)

    def test_xml_validation_task(self, mock_xml_validation_functions):
        """Test the main XML validation task with dynamic XSD selection."""
        # Arrange
        mock_xml_content = '''<?xml version="1.0"?>
            <Submission>
                <T5Summary>
                    <tx_yr>2025</tx_yr>
                </T5Summary>
            </Submission>'''
        mock_schema = MagicMock()
        mock_xml_validation_functions['fetch_xml'].return_value = mock_xml_content
        mock_xml_validation_functions['load_xsd'].return_value = mock_schema

        # Act
        xml_validation_task()

        # Assert - load_xsd should be called with the 2025 XSD path
        call_args = mock_xml_validation_functions['load_xsd'].call_args[0][0]
        assert 't5_xmlschm_2025' in call_args
        mock_xml_validation_functions['validate_xml'].assert_called_once_with(mock_xml_content, mock_schema)

    def test_list_xml_files(self, mock_storage_client, mock_pandas_dataframe, sample_xml_content):
        """Test XML parsing and CSV generation."""
        # Arrange
        mock_storage_client['blob'].open.return_value.__enter__.return_value = mock_open(read_data=sample_xml_content)()

        # Act
        list_xml_files('test-bucket', 'test/path.xml')

        # Assert
        mock_pandas_dataframe.to_csv.assert_called()

    def test_create_external_table_header(self, mock_bigquery_client, mock_create_external_table):
        """Test creating external table for header data."""
        # Act
        create_external_table_header()

        # Assert - Verify correct client instance is passed to external table creation
        assert mock_create_external_table.call_args[0][0] == mock_bigquery_client.return_value

    def test_generate_and_execute_insert_query(self, mock_bigquery_client, mock_bigquery_table):
        """Test generating and executing insert queries."""
        # Arrange
        mock_client_instance = mock_bigquery_client.return_value
        mock_client_instance.dataset.return_value.table.return_value = 'table_ref'
        mock_client_instance.get_table.return_value = mock_bigquery_table
        mock_client_instance.query.return_value.result.return_value = None

        # Act
        generate_and_execute_insert_query(
            'test-project',
            'external-project',
            'test-dataset',
            'external-table',
            'destination-table',
            'source-table'
        )

        call_args = mock_client_instance.query.call_args[0][0]
        assert 'CREATE OR REPLACE TABLE' in call_args
        assert 'INSERT INTO' in call_args
        assert 'column1, column2' in call_args

    def test_ingest_t5_header_data(self, mock_generate_and_execute_insert_query):
        """Test ingesting T5 header data."""
        # Act
        ingest_t5_header_data()

        # Assert - Verify correct configuration parameters are passed
        call_args = mock_generate_and_execute_insert_query.call_args
        # Check positional arguments (first 3)
        assert len(call_args[0]) == 3  # project_id, external_project_id, dataset_id
        # Check keyword arguments - these test the table mapping configuration
        assert call_args[1]['external_table_id'] == 'T5_TAX_RAW_HEADER_EXTERNAL'
        assert call_args[1]['destination_table_id'] == 'T5_TAX_RAW_HEADER'
        assert call_args[1]['source_table'] == 'T5_TAX_HEADER'

    def test_validate_t5_slip_data_success(self, mock_bigquery_client):
        """Test T5 slip data validation with no issues."""
        # Arrange
        mock_duplicate_result = MagicMock()
        mock_duplicate_result.result.return_value = [MagicMock(count=0)]
        mock_count_result = MagicMock()
        mock_count_result.result.return_value = [MagicMock(count=0)]
        mock_sin_result = MagicMock()
        mock_sin_result.result.return_value = [MagicMock(count=0)]

        mock_bigquery_client.return_value.query.side_effect = [mock_duplicate_result, mock_count_result, mock_sin_result]

        # Act
        validate_t5_slip_data()

        # Assert
        assert mock_bigquery_client.return_value.query.call_count == 3

    def test_validate_t5_slip_data_duplicate_file(self, mock_bigquery_client):
        """Test T5 slip data validation with duplicate file error."""
        # Arrange
        mock_duplicate_result = MagicMock()
        mock_duplicate_result.result.return_value = [MagicMock(count=1)]
        mock_bigquery_client.return_value.query.return_value = mock_duplicate_result

        # Act & Assert
        with pytest.raises(Exception, match="Validation failed.*duplicate file sbmt_ref_id found"):
            validate_t5_slip_data()

    def test_validate_t5_slip_data_duplicate_sin(self, mock_bigquery_client):
        """Test T5 slip data validation with duplicate sin error."""
        # Arrange
        mock_duplicate_result = MagicMock()
        mock_duplicate_result.result.return_value = [MagicMock(count=0)]
        mock_count_result = MagicMock()
        mock_count_result.result.return_value = [MagicMock(count=0)]
        mock_sin_result = MagicMock()
        mock_sin_result.result.return_value = [MagicMock(count=3)]

        mock_bigquery_client.return_value.query.side_effect = [mock_duplicate_result, mock_count_result, mock_sin_result]

        # Act & Assert
        with pytest.raises(Exception, match="Validation failed.*duplicate sin found"):
            validate_t5_slip_data()

    def test_validate_counts_and_amounts_success(self, mock_bigquery_client):
        """Test counts and amounts validation with successful match."""
        # Arrange
        mock_result = MagicMock()
        mock_result.result.return_value = [MagicMock(validation_status='Match')]
        mock_bigquery_client.return_value.query.return_value = mock_result

        # Act
        validate_counts_and_amounts()

    def test_validate_counts_and_amounts_failure(self, mock_bigquery_client):
        """Test counts and amounts validation with mismatch."""
        # Arrange
        mock_result = MagicMock()
        mock_result.result.return_value = [MagicMock(validation_status='Mismatch')]
        mock_bigquery_client.return_value.query.return_value = mock_result

        # Act & Assert
        with pytest.raises(Exception, match="Validation failed.*Counts or sums do not match"):
            validate_counts_and_amounts()

    def test_operator_types(self, mock_read_variable_or_file):
        """Test that operators are of correct types."""
        # Test PythonOperator
        assert isinstance(dag.get_task('validate_xml_with_xsd'), PythonOperator)
        assert isinstance(dag.get_task('transform_xml_to_CSV'), PythonOperator)
        assert isinstance(dag.get_task('create_external_table_header'), PythonOperator)

        # Test BigQueryInsertJobOperator
        assert isinstance(dag.get_task('t5_tax_slip_transformation'), BigQueryInsertJobOperator)
        assert isinstance(dag.get_task('insert_t5_tax_header'), BigQueryInsertJobOperator)

        # Test GCSToGCSOperator
        assert isinstance(dag.get_task('gcs_to_gcs'), GCSToGCSOperator)

        # Test TriggerDagRunOperator
        assert isinstance(dag.get_task('trigger_t5_generate_xml_dag'), TriggerDagRunOperator)
        assert isinstance(dag.get_task('trigger_temenos_t5_kafka_writer_dag'), TriggerDagRunOperator)

        # Test EmptyOperator
        assert isinstance(dag.get_task('start'), EmptyOperator)
        assert isinstance(dag.get_task('end'), EmptyOperator)

    def test_task_configurations(self, mock_read_variable_or_file):
        """Test specific task configurations."""
        # Test GCS to GCS task
        gcs_task = dag.get_task('gcs_to_gcs')
        assert gcs_task.source_bucket == "{{ dag_run.conf['bucket'] }}"
        assert gcs_task.source_object == "{{ dag_run.conf['name'] }}"

        # Test validation tasks have retries=0
        validate_xml_task = dag.get_task('validate_xml_with_xsd')
        validate_slip_task = dag.get_task('validate_t5_slip_data')
        validate_counts_task = dag.get_task('validate_counts_and_amounts')

        assert validate_xml_task.retries == 0
        assert validate_slip_task.retries == 0
        assert validate_counts_task.retries == 0

        # Test trigger tasks
        trigger_xml_task = dag.get_task('trigger_t5_generate_xml_dag')
        trigger_kafka_task = dag.get_task('trigger_temenos_t5_kafka_writer_dag')

        assert trigger_xml_task.trigger_dag_id == 't5_temenos_generate_xml'
        assert trigger_xml_task.wait_for_completion is True
        assert trigger_xml_task.retries == 0

        assert trigger_kafka_task.trigger_dag_id == 'statement_temenos_t5_kafka_writer'
        assert trigger_kafka_task.wait_for_completion is True

    def test_validate_summary_and_slip_bn_success(self, mock_run_bq_query):
        """Test validate_summary_and_slip_bn with matching BN values."""
        # Arrange
        # Mock trailer query result - returns summary BN
        mock_trailer_row = MagicMock()
        mock_trailer_row.bn = "123456789"
        mock_trailer_query_job = MagicMock()
        mock_trailer_query_job.result.return_value = [mock_trailer_row]

        # Mock slip query result - returns matching BN values
        mock_slip_row1 = MagicMock()
        mock_slip_row1.bn = "123456789"
        mock_slip_row2 = MagicMock()
        mock_slip_row2.bn = "123456789"
        mock_slip_query_job = MagicMock()
        mock_slip_query_job.result.return_value = [mock_slip_row1, mock_slip_row2]

        mock_run_bq_query.side_effect = [mock_trailer_query_job, mock_slip_query_job]

        # Act
        validate_summary_and_slip_bn()

        # Assert
        assert mock_run_bq_query.call_count == 2

    def test_validate_summary_and_slip_bn_trailer_not_found(self, mock_run_bq_query):
        """Test validate_summary_and_slip_bn when trailer record is not found."""
        # Arrange
        # Mock trailer query result - returns empty list
        mock_trailer_query_job = MagicMock()
        mock_trailer_query_job.result.return_value = []
        mock_run_bq_query.return_value = mock_trailer_query_job

        # Act & Assert
        with pytest.raises(AirflowFailException, match="Validation failed: T5Summary \\(trailer\\) record not found"):
            validate_summary_and_slip_bn()

    def test_validate_summary_and_slip_bn_summary_bn_null(self, mock_run_bq_query):
        """Test validate_summary_and_slip_bn when summary BN is null."""
        # Arrange
        # Mock trailer query result - returns null BN
        mock_trailer_row = MagicMock()
        mock_trailer_row.bn = None
        mock_trailer_query_job = MagicMock()
        mock_trailer_query_job.result.return_value = [mock_trailer_row]
        mock_run_bq_query.return_value = mock_trailer_query_job

        # Act & Assert
        with pytest.raises(AirflowFailException, match="Validation failed: T5Summary.bn is null"):
            validate_summary_and_slip_bn()

    def test_validate_summary_and_slip_bn_no_slip_bn_values(self, mock_run_bq_query):
        """Test validate_summary_and_slip_bn when no valid slip BN values are found."""
        # Arrange
        # Mock trailer query result - returns summary BN
        mock_trailer_row = MagicMock()
        mock_trailer_row.bn = "123456789"
        mock_trailer_query_job = MagicMock()
        mock_trailer_query_job.result.return_value = [mock_trailer_row]

        # Mock slip query result - returns empty list
        mock_slip_query_job = MagicMock()
        mock_slip_query_job.result.return_value = []

        mock_run_bq_query.side_effect = [mock_trailer_query_job, mock_slip_query_job]

        # Act & Assert
        with pytest.raises(AirflowFailException, match="Validation failed: No valid T5Slip.bn values found"):
            validate_summary_and_slip_bn()

    def test_validate_summary_and_slip_bn_mismatch(self, mock_run_bq_query):
        """Test validate_summary_and_slip_bn when BN values don't match."""
        # Arrange
        # Mock trailer query result - returns summary BN
        mock_trailer_row = MagicMock()
        mock_trailer_row.bn = "123456789"
        mock_trailer_query_job = MagicMock()
        mock_trailer_query_job.result.return_value = [mock_trailer_row]

        # Mock slip query result - returns mismatched BN values
        mock_slip_row1 = MagicMock()
        mock_slip_row1.bn = "123456789"
        mock_slip_row2 = MagicMock()
        mock_slip_row2.bn = "987654321"  # Mismatched BN
        mock_slip_query_job = MagicMock()
        mock_slip_query_job.result.return_value = [mock_slip_row1, mock_slip_row2]

        mock_run_bq_query.side_effect = [mock_trailer_query_job, mock_slip_query_job]

        # Act & Assert
        with pytest.raises(AirflowFailException, match="Validation failed: T5Summary.bn.*does not match all T5Slip.bn records"):
            validate_summary_and_slip_bn()

    def test_validate_summary_and_slip_bn_with_whitespace(self, mock_run_bq_query):
        """Test validate_summary_and_slip_bn handles whitespace correctly."""
        # Arrange
        # Mock trailer query result - returns summary BN with whitespace
        mock_trailer_row = MagicMock()
        mock_trailer_row.bn = " 123456789 "
        mock_trailer_query_job = MagicMock()
        mock_trailer_query_job.result.return_value = [mock_trailer_row]

        # Mock slip query result - returns BN values with/without whitespace
        mock_slip_row1 = MagicMock()
        mock_slip_row1.bn = "123456789"  # No whitespace
        mock_slip_row2 = MagicMock()
        mock_slip_row2.bn = " 123456789 "  # With whitespace
        mock_slip_query_job = MagicMock()
        mock_slip_query_job.result.return_value = [mock_slip_row1, mock_slip_row2]

        mock_run_bq_query.side_effect = [mock_trailer_query_job, mock_slip_query_job]

        # Act - should succeed as whitespace is stripped
        validate_summary_and_slip_bn()

        # Assert
        assert mock_run_bq_query.call_count == 2
