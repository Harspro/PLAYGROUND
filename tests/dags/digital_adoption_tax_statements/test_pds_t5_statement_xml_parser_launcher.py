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

from digital_adoption_tax_statements.pds_t5.t5_statement_pds_xml_parser_launcher import (
    dag, fetch_xml_from_gcs, load_xsd, validate_xml, xml_validation_task, list_xml_files,
    create_external_table_header, generate_and_execute_insert_query, insert_t5_header_data,
    validate_t5_slip_data, validate_counts_and_amounts, validate_summary_and_slip_bn,
    get_xsd_path, extract_tx_yr_from_xml, trim_xml_fields, save_xml_to_gcs
)


@pytest.fixture
def mock_read_variable_or_file():
    """Fixture for mocking read_variable_or_file function."""
    with patch('digital_adoption_tax_statements.pds_t5.t5_statement_pds_xml_parser_launcher.read_variable_or_file') as mock_func:
        mock_func.return_value = {
            'deployment_environment_name': 'test',
            'deploy_env_storage_suffix': 'test',
            'bq_query_location': 'US',
            'landing_zone_connection_id': 'test_connection'
        }
        yield mock_func


@pytest.fixture
def mock_read_yamlfile_env_suffix():
    """Fixture for mocking read_yamlfile_env_suffix function."""
    with patch('digital_adoption_tax_statements.pds_t5.t5_statement_pds_xml_parser_launcher.read_yamlfile_env_suffix') as mock_func:
        mock_func.return_value = {
            'project_id': 'test-project',
            'external_project_id': 'test-external-project',
            'staging_bucket': 'test-bucket',
            'pds_xml_file_path_gcs': 'test/path.xml',
            'pds_t5_tax_slip_csv': 'test/slip.csv',
            'pds_t5_tax_header_csv': 'test/header.csv',
            'pds_t5_tax_trailer_csv': 'test/trailer.csv',
            'outbound_bucket': 'test-outbound-bucket'
        }
        yield mock_func


@pytest.fixture
def mock_read_file_env():
    """Fixture for mocking read_file_env function."""
    with patch('digital_adoption_tax_statements.pds_t5.t5_statement_pds_xml_parser_launcher.read_file_env') as mock_func:
        mock_func.return_value = "SELECT * FROM test_table"
        yield mock_func


@pytest.fixture
def mock_bigquery_client():
    """Fixture for mocking BigQuery client."""
    with patch('digital_adoption_tax_statements.pds_t5.t5_statement_pds_xml_parser_launcher.bigquery.Client') as mock_client_class:
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        yield mock_client_class


@pytest.fixture
def mock_storage_client():
    """Fixture for mocking Google Cloud Storage client."""
    with patch('digital_adoption_tax_statements.pds_t5.t5_statement_pds_xml_parser_launcher.storage.Client') as mock_client_class:
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
    with patch('digital_adoption_tax_statements.pds_t5.t5_statement_pds_xml_parser_launcher.xmlschema.XMLSchema') as mock_schema_class:
        mock_schema = MagicMock()
        mock_schema_class.return_value = mock_schema
        yield mock_schema


@pytest.fixture
def mock_run_bq_query():
    """Fixture for mocking run_bq_query function."""
    with patch('digital_adoption_tax_statements.pds_t5.t5_statement_pds_xml_parser_launcher.run_bq_query') as mock_func:
        yield mock_func


@pytest.fixture
def mock_pandas_dataframe():
    """Fixture for mocking pandas DataFrame."""
    with patch('digital_adoption_tax_statements.pds_t5.t5_statement_pds_xml_parser_launcher.pd.DataFrame') as mock_df_class:
        mock_dataframe = MagicMock()
        mock_df_class.return_value = mock_dataframe
        yield mock_dataframe


@pytest.fixture
def mock_create_external_table():
    """Fixture for mocking create_external_table function."""
    with patch('digital_adoption_tax_statements.pds_t5.t5_statement_pds_xml_parser_launcher.create_external_table') as mock_func:
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
def sample_xml_with_long_fields():
    """Fixture providing XML content with fields that need trimming."""
    return '''<?xml version="1.0" encoding="UTF-8"?>
        <Submission>
            <T5Slip>
                <RCPNT_NM>
                    <snm>VeryLongSurnameExceeds20Characters</snm>
                    <gvn_nm>VeryLongGivenNameExceeds12Chars</gvn_nm>
                    <init>ABC</init>
                </RCPNT_NM>
                <RCPNT_ADDR>
                    <addr_l1_txt>This is a very long address line 1 that exceeds 30 characters limit</addr_l1_txt>
                    <addr_l2_txt>This is a very long address line 2 that exceeds 30 characters limit</addr_l2_txt>
                    <cty_nm>This is a very long city name that exceeds 28 chars</cty_nm>
                    <prov_cd>ONTARIO</prov_cd>
                    <cntry_cd>CANADA</cntry_cd>
                    <pstl_cd>M5V123456789</pstl_cd>
                </RCPNT_ADDR>
                <rcpnt_fi_acct_nbr>1234567890123456</rcpnt_fi_acct_nbr>
            </T5Slip>
            <T5Summary>
                <tx_yr>2025</tx_yr>
            </T5Summary>
        </Submission>'''


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


class TestPdsT5StatementXMLParserLauncher:
    """Test suite for PDS T5 Statement XML Parser Launcher DAG."""

    def test_dag_structure(self, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
        """Test DAG structure and configuration."""
        # Check DAG ID
        assert dag.dag_id == 'pds_t5_launcher'

        # Check default args
        assert dag.default_args['owner'] == 'team-digital-adoption-alerts'
        assert dag.default_args['capability'] == 'Account Management'
        assert dag.default_args['severity'] == 'P2'
        assert dag.default_args['max_active_runs'] == 1
        assert dag.default_args['retries'] == 3

        # Check schedule
        assert dag.schedule_interval is None

        # Check is_paused_upon_creation
        assert dag.is_paused_upon_creation is True

    def test_dag_tasks_and_dependencies(self, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
        """Test that all expected tasks are present in the DAG."""
        expected_task_ids = [
            'start',
            'gcs_to_gcs',
            'validate_xml_with_xsd',
            'transform_xml_to_CSV',
            'create_external_table_header',
            'create_external_table_trailer',
            'create_external_table_t5slip',
            'create_t5_header_ref',
            'validate_t5_slip_data',
            'validate_counts_and_amounts',
            'validate_summary_and_slip_bn',
            'ingest_t5_header_data',
            'ingest_t5_slip_data',
            'ingest_t5_trailer_data',
            'trigger_t5pds_kafka_dag',
            'copy_file_to_outbound',
            'end'
        ]

        actual_task_ids = set(dag.task_dict.keys())
        assert set(expected_task_ids).issubset(actual_task_ids)

    def test_validate_summary_and_slip_bn_success(self, mock_run_bq_query, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
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

    def test_validate_summary_and_slip_bn_trailer_not_found(self, mock_run_bq_query, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
        """Test validate_summary_and_slip_bn when trailer record is not found."""
        # Arrange
        # Mock trailer query result - returns empty list
        mock_trailer_query_job = MagicMock()
        mock_trailer_query_job.result.return_value = []
        mock_run_bq_query.return_value = mock_trailer_query_job

        # Act & Assert
        with pytest.raises(AirflowFailException, match="Validation failed: T5Summary \\(trailer\\) record not found"):
            validate_summary_and_slip_bn()

    def test_validate_summary_and_slip_bn_summary_bn_null(self, mock_run_bq_query, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
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

    def test_validate_summary_and_slip_bn_no_slip_bn_values(self, mock_run_bq_query, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
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

    def test_validate_summary_and_slip_bn_mismatch(self, mock_run_bq_query, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
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

    def test_validate_summary_and_slip_bn_with_whitespace(self, mock_run_bq_query, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
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

    def test_validate_summary_and_slip_bn_multiple_mismatches(self, mock_run_bq_query, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
        """Test validate_summary_and_slip_bn with multiple mismatched BN values."""
        # Arrange
        # Mock trailer query result - returns summary BN
        mock_trailer_row = MagicMock()
        mock_trailer_row.bn = "123456789"
        mock_trailer_query_job = MagicMock()
        mock_trailer_query_job.result.return_value = [mock_trailer_row]

        # Mock slip query result - returns multiple mismatched BN values
        mock_slip_row1 = MagicMock()
        mock_slip_row1.bn = "987654321"  # Mismatched
        mock_slip_row2 = MagicMock()
        mock_slip_row2.bn = "111222333"  # Mismatched
        mock_slip_query_job = MagicMock()
        mock_slip_query_job.result.return_value = [mock_slip_row1, mock_slip_row2]

        mock_run_bq_query.side_effect = [mock_trailer_query_job, mock_slip_query_job]

        # Act & Assert
        with pytest.raises(AirflowFailException, match="Validation failed: T5Summary.bn.*does not match all T5Slip.bn records"):
            validate_summary_and_slip_bn()

    def test_validate_summary_and_slip_bn_uses_pds_tables(self, mock_run_bq_query, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
        """Test that validate_summary_and_slip_bn uses PDS-specific table names."""
        # Arrange
        # Mock trailer query result
        mock_trailer_row = MagicMock()
        mock_trailer_row.bn = "123456789"
        mock_trailer_query_job = MagicMock()
        mock_trailer_query_job.result.return_value = [mock_trailer_row]

        # Mock slip query result
        mock_slip_row = MagicMock()
        mock_slip_row.bn = "123456789"
        mock_slip_query_job = MagicMock()
        mock_slip_query_job.result.return_value = [mock_slip_row]

        mock_run_bq_query.side_effect = [mock_trailer_query_job, mock_slip_query_job]

        # Act
        validate_summary_and_slip_bn()

        # Assert - Check that queries use PDS table names
        call_args_list = [call[0][0] for call in mock_run_bq_query.call_args_list]
        assert any('T5_TAX_PDS_TRAILER_EXTERNAL' in query for query in call_args_list)
        assert any('T5_TAX_PDS_SLIP_EXTERNAL' in query for query in call_args_list)

    def test_operator_types(self, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
        """Test that operators are of correct types."""
        # Test PythonOperator
        assert isinstance(dag.get_task('validate_xml_with_xsd'), PythonOperator)
        assert isinstance(dag.get_task('transform_xml_to_CSV'), PythonOperator)
        assert isinstance(dag.get_task('create_external_table_header'), PythonOperator)
        assert isinstance(dag.get_task('validate_summary_and_slip_bn'), PythonOperator)

        # Test BigQueryInsertJobOperator
        assert isinstance(dag.get_task('create_t5_header_ref'), BigQueryInsertJobOperator)

        # Test GCSToGCSOperator
        assert isinstance(dag.get_task('gcs_to_gcs'), GCSToGCSOperator)
        assert isinstance(dag.get_task('copy_file_to_outbound'), GCSToGCSOperator)

        # Test TriggerDagRunOperator
        assert isinstance(dag.get_task('trigger_t5pds_kafka_dag'), TriggerDagRunOperator)

        # Test EmptyOperator
        assert isinstance(dag.get_task('start'), EmptyOperator)
        assert isinstance(dag.get_task('end'), EmptyOperator)

    def test_task_configurations(self, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
        """Test specific task configurations."""
        # Test validation tasks have retries=0
        validate_slip_task = dag.get_task('validate_t5_slip_data')
        validate_counts_task = dag.get_task('validate_counts_and_amounts')
        validate_bn_task = dag.get_task('validate_summary_and_slip_bn')

        assert validate_slip_task.retries == 0
        assert validate_counts_task.retries == 0
        assert validate_bn_task.retries == 0

        # Test trigger task
        trigger_kafka_task = dag.get_task('trigger_t5pds_kafka_dag')
        assert trigger_kafka_task.trigger_dag_id == 'statement_pds_t5_kafka_writer'
        assert trigger_kafka_task.wait_for_completion is True

    # Tests for Dynamic XSD Selection
    def test_get_xsd_path_2024(self, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
        """Test get_xsd_path returns correct path for 2024."""
        # Act
        result = get_xsd_path('2024')

        # Assert
        assert 't5_xmlschm_2024' in result
        assert result.endswith('T619_T5.xsd')

    def test_get_xsd_path_2025(self, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
        """Test get_xsd_path returns correct path for 2025."""
        # Act
        result = get_xsd_path('2025')

        # Assert
        assert 't5_xmlschm_2025' in result
        assert result.endswith('T619_T5.xsd')

    def test_get_xsd_path_unsupported_year(self, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
        """Test get_xsd_path raises error for unsupported year."""
        # Act & Assert
        with pytest.raises(ValueError, match="Unsupported tax year: 2020"):
            get_xsd_path('2020')

    def test_extract_tx_yr_from_xml_valid(self, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
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

    def test_extract_tx_yr_from_xml_2024(self, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
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

    def test_extract_tx_yr_from_xml_with_whitespace(self, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
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

    def test_extract_tx_yr_from_xml_missing_element(self, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
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

    def test_extract_tx_yr_from_xml_invalid_xml(self, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
        """Test extract_tx_yr_from_xml raises error for invalid XML."""
        # Arrange
        xml_content = 'not valid xml content'

        # Act & Assert
        with pytest.raises(ValueError, match="Failed to parse XML"):
            extract_tx_yr_from_xml(xml_content)

    # Tests for XML Field Trimming
    def test_trim_xml_fields_snm(self, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
        """Test trim_xml_fields trims snm to 20 characters."""
        # Arrange
        xml_content = '''<?xml version="1.0"?>
            <Submission>
                <T5Slip>
                    <RCPNT_NM>
                        <snm>VeryLongSurnameExceeds20Characters</snm>
                    </RCPNT_NM>
                </T5Slip>
            </Submission>'''

        # Act
        result = trim_xml_fields(xml_content)

        # Assert
        assert 'VeryLongSurnameExcee' in result  # 20 chars
        assert 'VeryLongSurnameExceeds20Characters' not in result

    def test_trim_xml_fields_gvn_nm(self, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
        """Test trim_xml_fields trims gvn_nm to 12 characters."""
        # Arrange
        xml_content = '''<?xml version="1.0"?>
            <Submission>
                <T5Slip>
                    <RCPNT_NM>
                        <gvn_nm>VeryLongGivenNameExceeds</gvn_nm>
                    </RCPNT_NM>
                </T5Slip>
            </Submission>'''

        # Act
        result = trim_xml_fields(xml_content)

        # Assert
        assert 'VeryLongGive' in result  # 12 chars
        assert 'VeryLongGivenNameExceeds' not in result

    def test_trim_xml_fields_init(self, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
        """Test trim_xml_fields trims init to 1 character."""
        # Arrange
        xml_content = '''<?xml version="1.0"?>
            <Submission>
                <T5Slip>
                    <RCPNT_NM>
                        <init>ABC</init>
                    </RCPNT_NM>
                </T5Slip>
            </Submission>'''

        # Act
        result = trim_xml_fields(xml_content)

        # Assert
        assert '<init>A</init>' in result

    def test_trim_xml_fields_addr_l1_txt(self, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
        """Test trim_xml_fields trims addr_l1_txt to 30 characters."""
        # Arrange
        xml_content = '''<?xml version="1.0"?>
            <Submission>
                <T5Slip>
                    <RCPNT_ADDR>
                        <addr_l1_txt>This is a very long address line 1 that exceeds 30 characters</addr_l1_txt>
                    </RCPNT_ADDR>
                </T5Slip>
            </Submission>'''

        # Act
        result = trim_xml_fields(xml_content)

        # Assert
        assert 'This is a very long address li' in result  # 30 chars
        assert 'This is a very long address line 1 that exceeds 30 characters' not in result

    def test_trim_xml_fields_cty_nm(self, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
        """Test trim_xml_fields trims cty_nm to 28 characters."""
        # Arrange
        xml_content = '''<?xml version="1.0"?>
            <Submission>
                <T5Slip>
                    <RCPNT_ADDR>
                        <cty_nm>This is a very long city name that exceeds limit</cty_nm>
                    </RCPNT_ADDR>
                </T5Slip>
            </Submission>'''

        # Act
        result = trim_xml_fields(xml_content)

        # Assert
        assert 'This is a very long city nam' in result  # 28 chars
        assert 'This is a very long city name that exceeds limit' not in result

    def test_trim_xml_fields_prov_cd(self, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
        """Test trim_xml_fields trims prov_cd to 2 characters."""
        # Arrange
        xml_content = '''<?xml version="1.0"?>
            <Submission>
                <T5Slip>
                    <RCPNT_ADDR>
                        <prov_cd>ONTARIO</prov_cd>
                    </RCPNT_ADDR>
                </T5Slip>
            </Submission>'''

        # Act
        result = trim_xml_fields(xml_content)

        # Assert
        assert '<prov_cd>ON</prov_cd>' in result

    def test_trim_xml_fields_rcpnt_fi_acct_nbr(self, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
        """Test trim_xml_fields handles rcpnt_fi_acct_nbr (right-justified, zero-padded)."""
        # Arrange
        xml_content = '''<?xml version="1.0"?>
            <Submission>
                <T5Slip>
                    <rcpnt_fi_acct_nbr>1234567890123456</rcpnt_fi_acct_nbr>
                </T5Slip>
            </Submission>'''

        # Act
        result = trim_xml_fields(xml_content)

        # Assert - Should take last 12 chars and zero-pad
        assert '890123456789' not in result  # Not simple truncation
        # The last 12 chars of '1234567890123456' is '567890123456'
        assert '567890123456' in result

    def test_trim_xml_fields_preserves_short_values(self, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
        """Test trim_xml_fields preserves values that don't exceed limits."""
        # Arrange
        xml_content = '''<?xml version="1.0"?>
            <Submission>
                <T5Slip>
                    <RCPNT_NM>
                        <snm>Smith</snm>
                        <gvn_nm>John</gvn_nm>
                    </RCPNT_NM>
                    <RCPNT_ADDR>
                        <cty_nm>Toronto</cty_nm>
                        <prov_cd>ON</prov_cd>
                    </RCPNT_ADDR>
                </T5Slip>
            </Submission>'''

        # Act
        result = trim_xml_fields(xml_content)

        # Assert - Values should remain unchanged
        assert '<snm>Smith</snm>' in result
        assert '<gvn_nm>John</gvn_nm>' in result
        assert '<cty_nm>Toronto</cty_nm>' in result
        assert '<prov_cd>ON</prov_cd>' in result

    def test_trim_xml_fields_multiple_t5slips(self, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
        """Test trim_xml_fields handles multiple T5Slip elements."""
        # Arrange
        xml_content = '''<?xml version="1.0"?>
            <Submission>
                <T5Slip>
                    <RCPNT_NM>
                        <gvn_nm>VeryLongFirstName1</gvn_nm>
                    </RCPNT_NM>
                </T5Slip>
                <T5Slip>
                    <RCPNT_NM>
                        <gvn_nm>VeryLongFirstName2</gvn_nm>
                    </RCPNT_NM>
                </T5Slip>
            </Submission>'''

        # Act
        result = trim_xml_fields(xml_content)

        # Assert - Both should be trimmed to 12 chars
        assert 'VeryLongFirs' in result
        assert 'VeryLongFirstName1' not in result
        assert 'VeryLongFirstName2' not in result

    def test_save_xml_to_gcs(self, mock_storage_client, mock_read_variable_or_file, mock_read_yamlfile_env_suffix, mock_read_file_env):
        """Test save_xml_to_gcs uploads XML content to GCS."""
        # Arrange
        xml_content = '<test>xml content</test>'

        # Act
        save_xml_to_gcs(xml_content)

        # Assert
        mock_storage_client['blob'].upload_from_string.assert_called_once_with(
            xml_content, content_type='application/xml'
        )
