import pytest
from unittest.mock import patch, MagicMock, mock_open
import pandas as pd
import xml.etree.ElementTree as ET
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
import io
from datetime import datetime
import pytz
import xmlschema
from google.cloud import bigquery, storage

from digital_adoption_tax_statements.temenos_t5.t5_statement_xml_generate_launcher import (
    dag, get_xml_file_name, process_header, process_slips, process_trailer,
    upload_to_gcs, create_xml, fetch_xml_from_gcs, load_xsd, validate_xml,
    xml_validation_task, get_xsd_path, extract_tx_yr_from_xml
)


@pytest.fixture
def mock_bigquery_client():
    """Fixture for mocking BigQuery client."""
    with patch('digital_adoption_tax_statements.temenos_t5.t5_statement_xml_generate_launcher.bigquery.Client') as mock_client_class:
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        yield mock_client


@pytest.fixture
def mock_storage_client():
    """Fixture for mocking Google Cloud Storage client."""
    with patch('digital_adoption_tax_statements.temenos_t5.t5_statement_xml_generate_launcher.storage.Client') as mock_client_class:
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()

        mock_client_class.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_client.get_bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        yield {
            'client': mock_client,
            'bucket': mock_bucket,
            'blob': mock_blob
        }


@pytest.fixture
def mock_datetime():
    """Fixture for mocking datetime functionality."""
    with patch('digital_adoption_tax_statements.temenos_t5.t5_statement_xml_generate_launcher.datetime') as mock_dt:
        mock_now = MagicMock()
        mock_now.astimezone.return_value.strftime.return_value = '20241201120000'
        mock_dt.now.return_value = mock_now
        yield mock_dt


@pytest.fixture
def mock_read_env_filepattern():
    """Fixture for mocking read_env_filepattern function."""
    with patch('digital_adoption_tax_statements.temenos_t5.t5_statement_xml_generate_launcher.read_env_filepattern') as mock_func:
        mock_func.return_value = 'cra_outbound_t5_slip/pcb_cra_t5_slip_original_TEST123_20241201120000_test.xml'
        yield mock_func


@pytest.fixture
def mock_xmlschema():
    """Fixture for mocking xmlschema.XMLSchema."""
    with patch('digital_adoption_tax_statements.temenos_t5.t5_statement_xml_generate_launcher.xmlschema.XMLSchema') as mock_schema_class:
        mock_schema = MagicMock()
        mock_schema_class.return_value = mock_schema
        yield mock_schema


@pytest.fixture
def sample_dataframes():
    """Fixture providing sample test dataframes."""
    return {
        'header': pd.DataFrame({
            'sbmt_ref_id': ['TEST123'],
            'summ_cnt': [5],
            'lang_cd': ['E'],
            'TransmitterCountryCode': ['CA'],
            'bn9': ['123456789'],
            'bn15': ['123456789RT0001'],
            'trust': ['T'],
            'nr4': ['N'],
            'RepID': ['REP001'],
            'l1_nm': ['Test Company Inc.'],
            'cntc_nm': ['John Doe'],
            'cntc_area_cd': ['416'],
            'cntc_phn_nbr': ['1234567'],
            'cntc_extn_nbr': ['123'],
            'cntc_email_area': ['test@example.com'],
            'sec_cntc_email_area': ['backup@example.com']
        }),
        'slips': pd.DataFrame({
            'snm': ['Smith'],
            'gvn_nm': ['John'],
            'init': ['J'],
            'addr_l1_txt': ['123 Main St'],
            'addr_l2_txt': ['Apt 4'],
            'cty_nm': ['Toronto'],
            'prov_cd': ['ON'],
            'cntry_cd': ['CA'],
            'pstl_cd': ['M5V3A8'],
            'actl_elg_dvamt': [100.50],
            'actl_dvnd_amt': [200.75],
            'tx_elg_dvnd_pamt': [150.25],
            'cdn_int_amt': [50.00],
            'sin': ['123456789'],
            'rcp_nbr': [1]
        }),
        'trailer': pd.DataFrame({
            'l1_nm': ['Test Company Inc.'],
            'l2_nm': ['Tax Department'],
            'l3_nm': [''],
            'addr_l1_txt': ['456 Corporate Ave'],
            'addr_l2_txt': ['Suite 100'],
            'cty_nm': ['Toronto'],
            'prov_cd': ['ON'],
            'cntry_cd': ['CA'],
            'pstl_cd': ['M5V3A8'],
            'cntc_nm': ['Jane Smith'],
            'cntc_area_cd': ['416'],
            'cntc_phn_nbr': ['7654321'],
            'cntc_extn_nbr': ['456'],
            'tot_cdn_int_amt': [1000.00],
            'tot_actl_elg_dvamt': [2000.50],
            'tot_actl_dvnd_amt': [3000.75],
            'trnmtr_nbr': ['TRN001'],
            'taxyr': [2024],
            'rpt_tcd': ['ORIGINAL']
        })
    }


@pytest.fixture
def mock_task_instance():
    """Fixture for mocking Airflow task instance."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = 'test-file.xml'
    return mock_ti


@pytest.fixture
def mock_get_xml_file_name():
    """Fixture for mocking get_xml_file_name function."""
    with patch('digital_adoption_tax_statements.temenos_t5.t5_statement_xml_generate_launcher.get_xml_file_name') as mock_func:
        mock_func.return_value = 'test-file.xml'
        yield mock_func


@pytest.fixture
def mock_upload_to_gcs():
    """Fixture for mocking upload_to_gcs function."""
    with patch('digital_adoption_tax_statements.temenos_t5.t5_statement_xml_generate_launcher.upload_to_gcs') as mock_func:
        yield mock_func


@pytest.fixture
def mock_fetch_xml_from_gcs():
    """Fixture for mocking fetch_xml_from_gcs function."""
    with patch('digital_adoption_tax_statements.temenos_t5.t5_statement_xml_generate_launcher.fetch_xml_from_gcs') as mock_func:
        mock_func.return_value = '<test>xml content</test>'
        yield mock_func


@pytest.fixture
def mock_load_xsd():
    """Fixture for mocking load_xsd function."""
    with patch('digital_adoption_tax_statements.temenos_t5.t5_statement_xml_generate_launcher.load_xsd') as mock_func:
        mock_schema = MagicMock()
        mock_func.return_value = mock_schema
        yield mock_func


@pytest.fixture
def mock_validate_xml():
    """Fixture for mocking validate_xml function."""
    with patch('digital_adoption_tax_statements.temenos_t5.t5_statement_xml_generate_launcher.validate_xml') as mock_func:
        yield mock_func


class TestTemenosT5StatementXMLGenerateLauncher:
    """Test suite for T5 Statement XML Generate Launcher DAG."""

    def test_dag_structure(self):
        """Test DAG structure and configuration."""
        # Check DAG ID
        assert dag.dag_id == 't5_temenos_generate_xml'

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

        # Check catchup
        assert dag.catchup is False

    def test_dag_tasks_and_dependencies(self):
        """Test that all expected tasks are present in the DAG."""
        expected_task_ids = [
            'start',
            'create_xml',
            'validate_xml_with_xsd',
            'move_file_to_outbound_landing',
            'end'
        ]

        actual_task_ids = set(dag.task_dict.keys())
        assert set(expected_task_ids).issubset(actual_task_ids)

        # Check task dependencies
        for task, downstream_task in zip(expected_task_ids, expected_task_ids[1:]):
            current_task = dag.get_task(task)
            assert current_task.downstream_list[0].task_id == downstream_task

    def test_task_types(self):
        """Test that tasks are of correct types."""
        assert isinstance(dag.get_task('start'), EmptyOperator)
        assert isinstance(dag.get_task('create_xml'), PythonOperator)
        assert isinstance(dag.get_task('validate_xml_with_xsd'), PythonOperator)
        assert isinstance(dag.get_task('move_file_to_outbound_landing'), GCSToGCSOperator)
        assert isinstance(dag.get_task('end'), EmptyOperator)

    def test_get_xml_file_name(self, sample_dataframes, mock_datetime, mock_read_env_filepattern):
        """Test XML file name generation."""
        # Arrange
        df_header = sample_dataframes['header'][['sbmt_ref_id']]
        df_trailer = sample_dataframes['trailer'][['rpt_tcd']]

        # Act
        result = get_xml_file_name(df_header, df_trailer)

        # Assert
        assert 'pcb_cra_t5_slip_original_TEST123_20241201120000_test.xml' in result

    def test_process_header(self, sample_dataframes):
        """Test processing header data into XML."""
        # Arrange
        df_header = sample_dataframes['header']
        root = ET.Element('Submission')

        # Act
        process_header(df_header, root)

        # Assert
        t619 = root.find('T619')
        assert t619 is not None
        assert t619.find('sbmt_ref_id').text == 'TEST123'
        assert t619.find('summ_cnt').text == '5'
        assert t619.find('lang_cd').text == 'E'
        assert t619.find('TransmitterCountryCode').text == 'CA'

        # Check TransmitterAccountNumber
        trnmtr_acc = t619.find('TransmitterAccountNumber')
        assert trnmtr_acc is not None
        assert trnmtr_acc.find('bn9').text == '123456789'
        assert trnmtr_acc.find('bn15').text == '123456789RT0001'
        assert trnmtr_acc.find('trust').text == 'T'
        assert trnmtr_acc.find('nr4').text == 'N'

        # Check TransmitterRepID
        assert t619.find('TransmitterRepID').text == 'REP001'

        # Check TransmitterName
        trnmtr_nm = t619.find('TransmitterName')
        assert trnmtr_nm is not None
        assert trnmtr_nm.find('l1_nm').text == 'Test Company Inc.'

        # Check CNTC
        cntc = t619.find('CNTC')
        assert cntc is not None
        assert cntc.find('cntc_nm').text == 'John Doe'
        assert cntc.find('cntc_area_cd').text == '416'
        assert cntc.find('cntc_phn_nbr').text == '1234567'

    def test_process_slips(self, sample_dataframes):
        """Test processing slip data into XML."""
        # Arrange
        df_slips = sample_dataframes['slips']
        t5 = ET.Element('T5')

        # Act
        process_slips(df_slips, t5)

        # Assert
        t5_slip = t5.find('T5Slip')
        assert t5_slip is not None

        # Check RCPNT_NM
        rcpnt_nm = t5_slip.find('RCPNT_NM')
        assert rcpnt_nm is not None
        assert rcpnt_nm.find('snm').text == 'Smith'
        assert rcpnt_nm.find('gvn_nm').text == 'John'
        assert rcpnt_nm.find('init').text == 'J'

        # Check RCPNT_ADDR
        rcpnt_addr = t5_slip.find('RCPNT_ADDR')
        assert rcpnt_addr is not None
        assert rcpnt_addr.find('addr_l1_txt').text == '123 Main St'
        assert rcpnt_addr.find('cty_nm').text == 'Toronto'
        assert rcpnt_addr.find('prov_cd').text == 'ON'

        # Check T5_AMT with proper decimal formatting
        t5_amt = t5_slip.find('T5_AMT')
        assert t5_amt is not None
        assert t5_amt.find('actl_elg_dvamt').text == '100.50'
        assert t5_amt.find('actl_dvnd_amt').text == '200.75'
        assert t5_amt.find('cdn_int_amt').text == '50.00'

        # Check other elements
        assert t5_slip.find('sin').text == '123456789'
        assert t5_slip.find('rcp_nbr').text == '1'

    def test_process_trailer(self, sample_dataframes):
        """Test processing trailer data into XML."""
        # Arrange
        df_trailer = sample_dataframes['trailer']
        t5 = ET.Element('T5')

        # Act
        process_trailer(df_trailer, t5)

        # Assert
        t5_summary = t5.find('T5Summary')
        assert t5_summary is not None

        # Check FILR_NM
        filr_nm = t5_summary.find('FILR_NM')
        assert filr_nm is not None
        assert filr_nm.find('l1_nm').text == 'Test Company Inc.'
        assert filr_nm.find('l2_nm').text == 'Tax Department'

        # Check FILR_ADDR
        filr_addr = t5_summary.find('FILR_ADDR')
        assert filr_addr is not None
        assert filr_addr.find('addr_l1_txt').text == '456 Corporate Ave'
        assert filr_addr.find('cty_nm').text == 'Toronto'

        # Check CNTC
        t5_cntc = t5_summary.find('CNTC')
        assert t5_cntc is not None
        assert t5_cntc.find('cntc_nm').text == 'Jane Smith'
        assert t5_cntc.find('cntc_area_cd').text == '416'

        # Check T5_TAMT with proper decimal formatting
        t5_tamt = t5_summary.find('T5_TAMT')
        assert t5_tamt is not None
        assert t5_tamt.find('tot_cdn_int_amt').text == '1000.00'
        assert t5_tamt.find('tot_actl_elg_dvamt').text == '2000.50'

        # Check other elements
        assert t5_summary.find('trnmtr_nbr').text == 'TRN001'
        assert t5_summary.find('taxyr').text == '2024'

    def test_upload_to_gcs(self, mock_storage_client):
        """Test uploading XML to Google Cloud Storage."""
        # Arrange
        xml_content = '<test>xml content</test>'
        xml_bytes = io.BytesIO(xml_content.encode('utf-8'))
        bucket_name = 'test-bucket'
        file_name = 'test-file.xml'

        # Act
        upload_to_gcs(bucket_name, file_name, xml_bytes)

        mock_storage_client['blob'].upload_from_file.assert_called_once_with(xml_bytes, content_type='application/xml')

    def test_create_xml(self, mock_bigquery_client, sample_dataframes, mock_get_xml_file_name, mock_upload_to_gcs):
        """Test the main create_xml function."""
        # Arrange
        mock_bigquery_client.query.side_effect = [
            MagicMock(to_dataframe=lambda: sample_dataframes['slips']),
            MagicMock(to_dataframe=lambda: sample_dataframes['header']),
            MagicMock(to_dataframe=lambda: sample_dataframes['trailer'])
        ]

        # Act
        result = create_xml()

        # Assert
        assert result == 'test-file.xml'
        assert mock_bigquery_client.query.call_count == 3  # Ensures data was fetched from all required tables

    def test_fetch_xml_from_gcs(self, mock_storage_client, mock_task_instance):
        """Test fetching XML content from Google Cloud Storage."""
        # Arrange
        mock_storage_client['blob'].download_as_text.return_value = '<test>xml content</test>'
        kwargs = {'task_instance': mock_task_instance}

        # Act
        result = fetch_xml_from_gcs(**kwargs)

        # Assert
        assert result == '<test>xml content</test>'
        mock_task_instance.xcom_pull.assert_called_once_with(task_ids='create_xml')  # Ensures correct file path is retrieved

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

    def test_validate_xml_multiple_errors(self):
        """Test XML validation with multiple errors."""
        # Arrange
        mock_schema = MagicMock()
        mock_schema.is_valid.return_value = False

        mock_error1 = MagicMock()
        mock_error1.__str__.return_value = "Reason: Missing required element 'field1' Schema component: element"
        mock_error2 = MagicMock()
        mock_error2.__str__.return_value = "Reason: Invalid attribute 'attr2' Schema component: attribute"

        mock_schema.iter_errors.return_value = [mock_error1, mock_error2]
        xml_content = '<invalid>xml</invalid>'

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            validate_xml(xml_content, mock_schema)

        error_message = str(exc_info.value)
        assert "Missing required element 'field1'" in error_message
        assert "Invalid attribute 'attr2'" in error_message

    # Tests for Dynamic XSD Selection
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

    def test_xml_validation_task(self, mock_task_instance, mock_fetch_xml_from_gcs, mock_load_xsd, mock_validate_xml):
        """Test the main XML validation task with dynamic XSD selection."""
        # Arrange
        mock_xml_content = '''<?xml version="1.0"?>
            <Submission>
                <T5Summary>
                    <tx_yr>2025</tx_yr>
                </T5Summary>
            </Submission>'''
        mock_fetch_xml_from_gcs.return_value = mock_xml_content
        kwargs = {'task_instance': mock_task_instance}

        # Act
        xml_validation_task(**kwargs)

        # Assert - load_xsd should be called with the 2025 XSD path
        call_args = mock_load_xsd.call_args[0][0]
        assert 't5_xmlschm_2025' in call_args
        mock_validate_xml.assert_called_once_with(
            mock_fetch_xml_from_gcs.return_value,
            mock_load_xsd.return_value
        )

    def test_task_configurations(self):
        """Test specific task configurations."""
        # Test validation task has retries=0
        validate_xml_task = dag.get_task('validate_xml_with_xsd')
        assert validate_xml_task.retries == 0

        # Test GCS to GCS task configuration
        move_task = dag.get_task('move_file_to_outbound_landing')
        assert move_task.source_object == "{{ task_instance.xcom_pull(task_ids='create_xml') }}"
        assert move_task.destination_object == "{{ task_instance.xcom_pull(task_ids='create_xml') }}"
        assert move_task.move_object is True

    def test_process_header_with_null_values(self):
        """Test processing header data with null values."""
        # Arrange
        df_header = pd.DataFrame({
            'sbmt_ref_id': ['TEST123'],
            'summ_cnt': [None],  # null value
            'lang_cd': ['E'],
            'bn9': [None],  # null value
            'l1_nm': ['Test Company Inc.']
        })

        root = ET.Element('Submission')

        # Act
        process_header(df_header, root)

        # Assert
        t619 = root.find('T619')
        assert t619 is not None
        assert t619.find('sbmt_ref_id').text == 'TEST123'
        assert t619.find('lang_cd').text == 'E'

        # Null values should not create elements
        assert t619.find('summ_cnt') is None

        # TransmitterAccountNumber should still exist but without bn9
        trnmtr_acc = t619.find('TransmitterAccountNumber')
        assert trnmtr_acc is not None
        assert trnmtr_acc.find('bn9') is None

    def test_process_slips_with_null_values(self):
        """Test processing slip data with null values."""
        # Arrange
        df_slips = pd.DataFrame({
            'snm': ['Smith'],
            'gvn_nm': [None],  # null value
            'actl_elg_dvamt': [100.50],
            'actl_dvnd_amt': [None],  # null value
            'sin': ['123456789']
        })

        t5 = ET.Element('T5')

        # Act
        process_slips(df_slips, t5)

        # Assert
        t5_slip = t5.find('T5Slip')
        assert t5_slip is not None

        rcpnt_nm = t5_slip.find('RCPNT_NM')
        assert rcpnt_nm.find('snm').text == 'Smith'
        assert rcpnt_nm.find('gvn_nm') is None  # null value not included

        t5_amt = t5_slip.find('T5_AMT')
        assert t5_amt.find('actl_elg_dvamt').text == '100.50'
        assert t5_amt.find('actl_dvnd_amt') is None  # null value not included

    def test_create_xml_bigquery_queries(self, mock_bigquery_client, mock_get_xml_file_name, mock_upload_to_gcs):
        """Test that create_xml makes the correct BigQuery queries."""
        # Arrange - Mock minimal dataframes with required columns to avoid IndexError
        mock_df_slips = pd.DataFrame({'snm': ['Test']})
        mock_df_header = pd.DataFrame({'sbmt_ref_id': ['TEST123']})
        mock_df_trailer = pd.DataFrame({'rpt_tcd': ['ORIGINAL']})

        mock_bigquery_client.query.side_effect = [
            MagicMock(to_dataframe=lambda: mock_df_slips),
            MagicMock(to_dataframe=lambda: mock_df_header),
            MagicMock(to_dataframe=lambda: mock_df_trailer)
        ]

        # Act
        create_xml()

        # Assert
        assert mock_bigquery_client.query.call_count == 3

        # Check the queries contain expected table references
        call_args_list = [call[0][0] for call in mock_bigquery_client.query.call_args_list]

        # Check slips query
        assert 'T5_ENRICHED_TAX_SLIP' in call_args_list[0]
        assert 'EXCEPT(create_date, sbmt_ref_id)' in call_args_list[0]

        # Check header query
        assert 'T5_TAX_RAW_HEADER' in call_args_list[1]
        assert 'EXCEPT(create_date)' in call_args_list[1]

        # Check trailer query
        assert 'T5_TAX_RAW_TRAILER' in call_args_list[2]
        assert 'EXCEPT(create_date, sbmt_ref_id)' in call_args_list[2]
