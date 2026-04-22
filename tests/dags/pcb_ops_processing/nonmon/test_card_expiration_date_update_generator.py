import pytest
from unittest.mock import patch, MagicMock
import logging
from datetime import datetime
from airflow.exceptions import AirflowFailException

from pcb_ops_processing.nonmon.card_expiration_date_update_generator import CardExpirationDateUpdateFileGenerator
import util.constants as consts
from pcb_ops_processing.nonmon.abc.base_nonmon_file_generator import HEADER_IDNTFR, TRAILER_IDNTFR


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
        'dag_id': 'test_card_expiration_date_dag',
        'default_args': {
            'owner': 'test_owner',
            'retries': 1
        }
    }


@pytest.fixture
def mock_base_dependencies():
    """Fixture to mock all base class dependencies"""
    with patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file') as mock_read_var, \
         patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env') as mock_read_yaml, \
         patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings') as mock_settings:

        mock_settings.DAGS_FOLDER = '/test/dags'
        yield {
            'mock_read_var': mock_read_var,
            'mock_read_yaml': mock_read_yaml,
            'mock_settings': mock_settings
        }


@pytest.fixture
def generator(mock_base_dependencies, mock_gcp_config, mock_config):
    """Fixture to create CardExpirationDateUpdateFileGenerator instance"""
    mock_base_dependencies['mock_read_var'].return_value = mock_gcp_config
    mock_base_dependencies['mock_read_yaml'].return_value = mock_config

    return CardExpirationDateUpdateFileGenerator('test_config.yaml')


class TestCardExpirationDateUpdateFileGenerator:
    """Test cases for CardExpirationDateUpdateFileGenerator class"""

    def test_generate_nonmon_record_valid_input(self, generator):
        """Test generate_nonmon_record with valid card expiration data"""
        # Test with valid card expiration data
        account_id = 1234
        card_expiration_date = '2025-12-31'
        original_row = [str(account_id), card_expiration_date]

        result = generator.generate_nonmon_record(account_id, original_row)

        # Verify the record structure
        assert len(result) == 596  # Standard record length
        assert result.startswith("0000000000001234")  # Zero-padded account ID (16 chars)
        assert "@@@" in result  # Filler
        assert "SDDATA" in result  # TSYS ID
        assert "176" in result  # Record type
        assert "026504" in result  # Field indicator

        # Verify Julian date conversion (2025-12-31 = 2025365)
        expected_julian = '2025365'
        assert expected_julian in result

    def test_generate_nonmon_record_different_date_format(self, generator):
        """Test generate_nonmon_record with different valid date"""
        account_id = 98765
        card_expiration_date = '2024-01-01'
        original_row = [str(account_id), card_expiration_date]

        result = generator.generate_nonmon_record(account_id, original_row)

        # Verify Julian date conversion (2024-01-01 = 2024001)
        expected_julian = '2024001'
        assert expected_julian in result
        assert len(result) == 596

    def test_generate_nonmon_record_none_original_row_raises_error(self, generator):
        """Test that None original_row raises an AirflowFailException"""
        account_id = 22222
        original_row = None

        with pytest.raises(AirflowFailException) as exc_info:
            generator.generate_nonmon_record(account_id, original_row)

        assert "The input row should contain account_id, card_expiration_date." in str(exc_info.value)
        assert "Account ID: 22222" in str(exc_info.value)
        assert "Row: None" in str(exc_info.value)

    def test_generate_nonmon_record_empty_list_raises_error(self, generator):
        """Test that empty list raises an AirflowFailException"""
        account_id = 33333
        original_row = []

        with pytest.raises(AirflowFailException) as exc_info:
            generator.generate_nonmon_record(account_id, original_row)

        assert "The input row should contain account_id, card_expiration_date." in str(exc_info.value)
        assert "Account ID: 33333" in str(exc_info.value)
        assert "Row: []" in str(exc_info.value)

    def test_generate_nonmon_record_insufficient_data_raises_error(self, generator):
        """Test that insufficient data in row raises an AirflowFailException"""
        account_id = 44444
        original_row = [str(account_id)]  # Missing card expiration date

        with pytest.raises(AirflowFailException) as exc_info:
            generator.generate_nonmon_record(account_id, original_row)

        assert "The input row should contain account_id, card_expiration_date." in str(exc_info.value)
        assert "Account ID: 44444" in str(exc_info.value)

    def test_generate_nonmon_record_non_list_original_row_raises_error(self, generator):
        """Test that non-list original_row raises an AirflowFailException"""
        account_id = 55555
        original_row = "not_a_list"

        with pytest.raises(AirflowFailException) as exc_info:
            generator.generate_nonmon_record(account_id, original_row)

        assert "The input row should contain account_id, card_expiration_date." in str(exc_info.value)
        assert "Account ID: 55555" in str(exc_info.value)

    def test_generate_nonmon_record_pads_account_id(self, generator):
        """Test that account ID is properly zero-padded to 16 characters"""
        account_id = 123  # Short account ID
        card_expiration_date = '2025-06-15'
        original_row = [str(account_id), card_expiration_date]

        result = generator.generate_nonmon_record(account_id, original_row)

        # Account ID should be zero-padded to 16 characters
        assert result.startswith("0000000000000123")

    def test_generate_nonmon_record_with_whitespace_in_date(self, generator):
        """Test generate_nonmon_record handles whitespace in date"""
        account_id = 77777
        card_expiration_date = '  2025-03-15  '  # Date with whitespace
        original_row = [str(account_id), card_expiration_date]

        result = generator.generate_nonmon_record(account_id, original_row)

        # Should handle whitespace and convert correctly (2025-03-15 = 2025074)
        expected_julian = '2025074'
        assert expected_julian in result
        assert len(result) == 596

    def test_generate_nonmon_record_with_leap_year_date(self, generator):
        """Test generate_nonmon_record with leap year date"""
        account_id = 88888
        card_expiration_date = '2024-02-29'  # Leap year date
        original_row = [str(account_id), card_expiration_date]

        result = generator.generate_nonmon_record(account_id, original_row)

        # 2024-02-29 = 2024060 (60th day of leap year)
        expected_julian = '2024060'
        assert expected_julian in result

    def test_build_nonmon_table(self, generator):
        """Test build_nonmon_table generates correct DDL for card expiration"""
        table_id = 'test_project.test_dataset.card_expiration_table'
        result = generator.build_nonmon_table(table_id)

        # Verify DDL structure for card expiration
        assert f"CREATE TABLE IF NOT EXISTS `{table_id}`" in result
        assert "ACCOUNT_ID STRING" in result
        assert "FILLER STRING" in result
        assert "TSYS_ID STRING" in result
        assert "RECORD_TYPE STRING" in result
        assert "FIELD_INDICATOR STRING" in result
        assert "CARD_EXPIRATION_JULIAN_DATE STRING" in result
        assert "REC_LOAD_TIMESTAMP DATETIME" in result
        assert "FILE_NAME STRING" in result

    def test_build_nonmon_query_single_view(self, generator):
        """Test build_nonmon_query with a single view for card expiration"""
        transformed_views = [
            {
                'id': 'test_project.test_dataset.card_expiration_view',
                'columns': 'record_data, file_name'
            }
        ]
        table_id = 'test_project.test_dataset.card_expiration_table'

        result = generator.build_nonmon_query(transformed_views, table_id)

        # Verify query structure for card expiration
        assert f"INSERT INTO {table_id}" in result
        assert "SUBSTR(record_data, 1, 16)   AS ACCOUNT_ID" in result
        assert "SUBSTR(record_data, 17, 3)   AS FILLER" in result
        assert "SUBSTR(record_data, 20, 6)   AS TSYS_ID" in result
        assert "SUBSTR(record_data, 26, 3)   AS RECORD_TYPE" in result
        assert "SUBSTR(record_data, 29, 6)  AS FIELD_INDICATOR" in result
        assert "SUBSTR(record_data, 35, 7)  AS CARD_EXPIRATION_JULIAN_DATE" in result
        assert "CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP" in result
        assert "file_name AS FILE_NAME" in result
        assert "`test_project.test_dataset.card_expiration_view`" in result

    def test_build_nonmon_query_multiple_views(self, generator):
        """Test build_nonmon_query with multiple views for card expiration"""
        transformed_views = [
            {
                'id': 'test_project.test_dataset.card_expiration_view1',
                'columns': 'record_data, file_name'
            },
            {
                'id': 'test_project.test_dataset.card_expiration_view2',
                'columns': 'record_data, file_name'
            }
        ]
        table_id = 'test_project.test_dataset.card_expiration_table'

        result = generator.build_nonmon_query(transformed_views, table_id)

        # Verify UNION ALL structure
        assert f"INSERT INTO {table_id}" in result
        assert "UNION ALL" in result
        assert "`test_project.test_dataset.card_expiration_view1`" in result
        assert "`test_project.test_dataset.card_expiration_view2`" in result
        assert result.count("SELECT") == 2  # Two SELECT statements

    @patch('pcb_ops_processing.nonmon.card_expiration_date_update_generator.logging.getLogger')
    def test_build_nonmon_query_logs_debug_info(self, mock_get_logger, generator):
        """Test that build_nonmon_query logs appropriate debug information"""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        transformed_views = [
            {
                'id': 'test_project.test_dataset.card_expiration_view',
                'columns': 'record_data, file_name'
            }
        ]
        table_id = 'test_project.test_dataset.card_expiration_table'

        generator.build_nonmon_query(transformed_views, table_id)

        # Verify logging calls
        assert mock_logger.info.call_count >= 2  # At least 2 info calls

        # Check that the expected content is in the logged messages
        logged_messages = [str(call.args[0]) for call in mock_logger.info.call_args_list]

        assert any("Building query for payment hierarchy view: test_project.test_dataset.card_expiration_view" in msg for msg in logged_messages)
        assert any("record :: record_data" in msg for msg in logged_messages)
        assert any("filename :: file_name" in msg for msg in logged_messages)
        assert any("Final combined query for payment hierarchy:" in msg for msg in logged_messages)

    def test_build_nonmon_query_filters_header_trailer(self, generator):
        """Test that build_nonmon_query includes WHERE clause to filter header/trailer records"""
        transformed_views = [
            {
                'id': 'test_project.test_dataset.card_expiration_view',
                'columns': 'record_data, file_name'
            }
        ]
        table_id = 'test_project.test_dataset.card_expiration_table'

        result = generator.build_nonmon_query(transformed_views, table_id)

        # Verify WHERE clause filters header and trailer records
        assert "WHERE NOT REGEXP_CONTAINS(record_data, r'" in result
        assert HEADER_IDNTFR in result or TRAILER_IDNTFR in result  # Should reference the constants


class TestCardExpirationDateUpdateFileGeneratorEdgeCases:
    """Test edge cases for CardExpirationDateUpdateFileGenerator"""

    def test_generate_nonmon_record_with_very_long_account_id(self, generator):
        """Test generate_nonmon_record with account ID longer than 16 digits"""
        account_id = 123456789012345678901  # 21 digits - longer than 16
        card_expiration_date = '2025-12-31'
        original_row = [str(account_id), card_expiration_date]

        result = generator.generate_nonmon_record(account_id, original_row)

        # Should use the zfill result which will take all digits for long numbers
        account_id_str = str(account_id).zfill(16)
        assert result.startswith(account_id_str)
        assert len(result) == 596

    def test_build_nonmon_query_empty_views_list(self, generator):
        """Test build_nonmon_query with empty views list"""
        transformed_views = []
        table_id = 'test_project.test_dataset.card_expiration_table'

        result = generator.build_nonmon_query(transformed_views, table_id)

        # Should still have INSERT INTO but empty content
        assert f"INSERT INTO {table_id}" in result
        assert "UNION ALL" not in result  # No UNION ALL with empty list

    def test_generate_nonmon_record_invalid_date_format_raises_error(self, generator):
        """Test that invalid date format raises a ValueError"""
        account_id = 99999
        card_expiration_date = 'invalid-date'
        original_row = [str(account_id), card_expiration_date]

        with pytest.raises(ValueError) as exc_info:
            generator.generate_nonmon_record(account_id, original_row)

        # Should raise ValueError from datetime.strptime
        assert "time data 'invalid-date' does not match format '%Y-%m-%d'" in str(exc_info.value)

    def test_generate_nonmon_record_with_numeric_account_id(self, generator):
        """Test generate_nonmon_record when account_id is passed as integer"""
        account_id = 123456  # Integer instead of string
        card_expiration_date = '2025-12-31'
        original_row = [str(account_id), card_expiration_date]

        result = generator.generate_nonmon_record(account_id, original_row)

        # Should work with numeric account ID
        assert result.startswith("0000000000123456")
        assert len(result) == 596

    def test_build_nonmon_query_with_different_column_names(self, generator):
        """Test build_nonmon_query handles different column names correctly"""
        transformed_views = [
            {
                'id': 'test_project.test_dataset.card_expiration_view',
                'columns': 'different_record_col, different_file_col'
            }
        ]
        table_id = 'test_project.test_dataset.card_expiration_table'

        result = generator.build_nonmon_query(transformed_views, table_id)

        # Should use the specified column names
        assert "SUBSTR(different_record_col," in result
        assert "different_file_col AS FILE_NAME" in result
