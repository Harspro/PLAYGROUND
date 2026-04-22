from unittest.mock import patch
from airflow.exceptions import AirflowFailException

import pytest

import util.constants as consts
from pcb_ops_processing.nonmon.abc.base_nonmon_file_generator import HEADER_IDNTFR, TRAILER_IDNTFR
from pcb_ops_processing.nonmon.tw40_start_date_update_generator import TW40StartDateUpdateFileGenerator


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
        'dag_id': 'test_tw40_start_date_dag',
        'default_args': {
            'owner': 'test_owner',
            'retries': 1
        }
    }


class TestTW40StartDateUpdateFileGenerator:
    """Test cases for TW40StartDateUpdateFileGenerator class"""

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_generate_nonmon_record_valid_input(self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config,
                                                mock_config):
        """Test generate_nonmon_record with valid account data"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = TW40StartDateUpdateFileGenerator('test_config.yaml')

        # Test with valid account data
        account_id = "13951111"
        account_open_date = "2025-01-15"
        offset_days = "50"
        original_row = [account_id, account_open_date, offset_days]

        result = generator.generate_nonmon_record(account_id, original_row)

        # Verify the record structure
        assert len(result) == 596  # Standard record length
        assert result.startswith("IIIIII0001395111100")  # Account Access Number
        assert "ts2opr" in result  # Operator ID
        assert "245" in result  # Record Type
        assert "245002" in result  # Field Indicator 1
        assert "40" in result  # Tiered Watch Reason
        assert "245107" in result  # Field Indicator 2
        assert "2025065" in result  # TW40 Start Date (Julian: 2025-03-06 = 2025065)
        assert "245207" in result  # Field Indicator 3
        assert "9999999" in result  # TW40 Stop Date

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_generate_nonmon_record_empty_data_raises_error(self, mock_settings, mock_read_yaml, mock_read_var,
                                                            mock_gcp_config, mock_config):
        """Test that empty account data raises a AirflowFailException"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = TW40StartDateUpdateFileGenerator('test_config.yaml')

        # Test with empty account data
        account_id = "13951111"
        original_row = None

        with pytest.raises(AirflowFailException) as exc_info:
            generator.generate_nonmon_record(account_id, original_row)

        assert "Account data including open date and offset days is required." in str(exc_info.value)
        assert "Account ID: 13951111" in str(exc_info.value)

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_generate_nonmon_record_invalid_date_format_raises_error(self, mock_settings, mock_read_yaml, mock_read_var,
                                                                     mock_gcp_config, mock_config):
        """Test that invalid date format raises a AirflowFailException"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = TW40StartDateUpdateFileGenerator('test_config.yaml')

        # Test with invalid date format
        account_id = "13951111"
        original_row = [account_id, "invalid-date", "50"]

        with pytest.raises(AirflowFailException) as exc_info:
            generator.generate_nonmon_record(account_id, original_row)

        assert "Invalid account open date format" in str(exc_info.value)

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_generate_nonmon_record_different_date_formats(self, mock_settings, mock_read_yaml, mock_read_var,
                                                           mock_gcp_config, mock_config):
        """Test generate_nonmon_record with different date formats"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = TW40StartDateUpdateFileGenerator('test_config.yaml')

        # Test with YYYY/MM/DD format
        account_id = "00013951111"
        original_row = [account_id, "2025/01/15", "50"]
        result1 = generator.generate_nonmon_record(account_id, original_row)
        assert "2025065" in result1  # Julian date for 2025-03-06

        # Test with YYYYMMDD format
        original_row = [account_id, "20250115", "50"]
        result2 = generator.generate_nonmon_record(account_id, original_row)
        assert "2025065" in result2  # Same Julian date

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_build_nonmon_table(self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config, mock_config):
        """Test build_nonmon_table method"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = TW40StartDateUpdateFileGenerator('test_config.yaml')
        table_id = "test_project.test_dataset.test_table"

        result = generator.build_nonmon_table(table_id)

        # Verify the DDL contains expected fields
        assert "CREATE TABLE IF NOT EXISTS" in result
        assert "FILLER_1 STRING" in result
        assert "ACCOUNT_NO STRING" in result
        assert "FILLER_2 STRING" in result
        assert "OPERATOR_ID STRING" in result
        assert "RECORD_TYPE STRING" in result
        assert "FIELD_INDICATOR_1 STRING" in result
        assert "TIERED_WATCH_REASON STRING" in result
        assert "FIELD_INDICATOR_2 STRING" in result
        assert "TIERED_WATCH_START_DATE STRING" in result
        assert "FIELD_INDICATOR_3 STRING" in result
        assert "TIERED_WATCH_STOP_DATE STRING" in result
        assert "REC_LOAD_TIMESTAMP DATETIME" in result
        assert "FILE_NAME STRING" in result

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_build_nonmon_query(self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config, mock_config):
        """Test build_nonmon_query method"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = TW40StartDateUpdateFileGenerator('test_config.yaml')
        table_id = "test_project.test_dataset.test_table"
        # Mock transformed views
        transformed_views = [
            {
                'id': 'test_project.test_dataset.test_view_1',
                'columns': 'record_data, file_name'
            }
        ]

        result = generator.build_nonmon_query(transformed_views, table_id)

        # Verify the query structure
        assert f"INSERT INTO {table_id}" in result
        assert "SUBSTR(record_data, 1, 6)    AS FILLER_1" in result
        assert "SUBSTR(record_data, 7, 11)   AS ACCOUNT_NO" in result
        assert "SUBSTR(record_data, 18, 2)   AS FILLER_2" in result
        assert "SUBSTR(record_data, 20, 6)   AS OPERATOR_ID" in result
        assert "SUBSTR(record_data, 26, 3)   AS RECORD_TYPE" in result
        assert "SUBSTR(record_data, 35, 2)   AS TIERED_WATCH_REASON" in result
        assert "SUBSTR(record_data, 43, 7)   AS TIERED_WATCH_START_DATE" in result
        assert "SUBSTR(record_data, 56, 7)   AS TIERED_WATCH_STOP_DATE" in result
        assert "CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP" in result
        assert "file_name AS FILE_NAME" in result
        assert f"WHERE NOT REGEXP_CONTAINS(record_data, r'{HEADER_IDNTFR}|{TRAILER_IDNTFR}')" in result

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_julian_date_calculation(self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config, mock_config):
        """Test Julian date calculation for various dates"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = TW40StartDateUpdateFileGenerator('test_config.yaml')

        # Test cases: (account_open_date, expected_julian_date)
        test_cases = [
            ("2025-01-01", "2025051"),  # Jan 1 + 50 days = Feb 20 (day 51)
            ("2025-02-15", "2025096"),  # Feb 15 + 50 days = Apr 6 (day 96)
            ("2025-12-01", "2026020"),  # Dec 1 + 50 days = Jan 20 next year (day 20)
        ]

        for account_open_date, expected_julian in test_cases:
            account_id = "00013951111"
            offset_days = "50"
            original_row = [account_id, account_open_date, offset_days]

            result = generator.generate_nonmon_record(account_id, original_row)
            assert expected_julian in result, f"Expected {expected_julian} for {account_open_date}"

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_generate_nonmon_record_with_offset_days(self, mock_settings, mock_read_yaml, mock_read_var,
                                                     mock_gcp_config, mock_config):
        """Test generate_nonmon_record with different offset_days values"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = TW40StartDateUpdateFileGenerator('test_config.yaml')
        account_id = "13951111"

        # Case 1: Explicit offset = 10 days
        original_row = [account_id, "2025-01-01", "10"]
        result = generator.generate_nonmon_record(account_id, original_row)
        assert "2025011" in result  # Jan 1 + 10 days = Jan 11 (Julian 011)

        # Case 2: Empty offset string -> defaults to 50
        original_row = [account_id, "2025-01-01", ""]
        with pytest.raises(AirflowFailException) as exc_info:
            generator.generate_nonmon_record(account_id, original_row)
        assert "Account data including open date and offset days is required." in str(exc_info.value)

        # Case 3: Invalid offset -> defaults to 50
        original_row = [account_id, "2025-01-01", "abc"]
        with pytest.raises(AirflowFailException) as exc_info:
            generator.generate_nonmon_record(account_id, original_row)
        assert "Account data including open date and offset days is required." in str(exc_info.value)

        # Case 4: Zero offset -> stays as Jan 1
        original_row = [account_id, "2025-01-01", "0"]
        result = generator.generate_nonmon_record(account_id, original_row)
        assert "2025001" in result  # Jan 1 = day 001
