import pytest
from unittest.mock import patch, MagicMock
import logging
from airflow.exceptions import AirflowFailException

from pcb_ops_processing.nonmon.product_change_generator import (
    ProductChangeFileGenerator,
    VALID_CHANGE_OPTION_SETS,
    CARD_DELIVERY,
    PIN_MAILER_TYPE,
    ALT_SHIPPING_ADDRESS_IND
)
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
        'dag_id': 'test_product_change_dag',
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
    """Fixture to create ProductChangeFileGenerator instance"""
    mock_base_dependencies['mock_read_var'].return_value = mock_gcp_config
    mock_base_dependencies['mock_read_yaml'].return_value = mock_config

    return ProductChangeFileGenerator('test_config.yaml')


class TestProductChangeFileGeneratorFormatAccountId:
    """Test cases for _format_account_id method"""

    def test_format_account_id_standard_10_digit(self, generator):
        """Test formatting a standard 10-digit account ID"""
        account_id = "2222222217"
        result = generator._format_account_id(account_id)

        # Should be: IIIIIIII + 2222222217 + 00 = 19 chars
        assert result == "IIIIIII222222221700"
        assert len(result) == 19
        assert result.endswith("00")
        assert result.startswith("I")

    def test_format_account_id_short_account(self, generator):
        """Test formatting a short account ID (padded with more I's)"""
        account_id = "12345"
        result = generator._format_account_id(account_id)

        # Should be padded with I's to reach 19 chars
        assert result == "IIIIIIIIIIII1234500"
        assert len(result) == 19
        assert result.endswith("00")

    def test_format_account_id_long_account(self, generator):
        """Test formatting a longer account ID"""
        account_id = "1234567890123456"
        result = generator._format_account_id(account_id)

        # Should append 00 and potentially truncate/adjust
        assert result == "I123456789012345600"
        assert len(result) == 19
        assert result.endswith("00")

    def test_format_account_id_with_whitespace(self, generator):
        """Test formatting account ID with leading/trailing whitespace"""
        account_id = "  2222222217  "
        result = generator._format_account_id(account_id)

        # Should strip whitespace first
        assert result == "IIIIIII222222221700"
        assert len(result) == 19

    def test_format_account_id_single_digit(self, generator):
        """Test formatting a single digit account ID"""
        account_id = "5"
        result = generator._format_account_id(account_id)

        # Should be heavily padded with I's
        assert result == "IIIIIIIIIIIIIIII500"
        assert len(result) == 19
        assert result.endswith("00")

    def test_format_account_id_numeric_input(self, generator):
        """Test formatting when account ID is passed as integer"""
        account_id = 2222222217
        result = generator._format_account_id(account_id)

        assert result == "IIIIIII222222221700"
        assert len(result) == 19


class TestProductChangeFileGeneratorGenerateNonmonRecord:
    """Test cases for generate_nonmon_record method"""

    def test_generate_nonmon_record_valid_input(self, generator):
        """Test generate_nonmon_record with valid product change data"""
        account_id = "2222222217"
        change_option_set = "0006"  # PCB SILVER CHIP/PIN
        cycle_day = "15"
        original_row = [account_id, change_option_set, cycle_day]

        result = generator.generate_nonmon_record(account_id, original_row)

        # Verify the record structure
        assert len(result) == 596  # Standard record length
        assert "IIIIIII222222221700" in result  # Formatted account ID
        # assert "@@@" in result  # Filler
        assert "260" in result  # Record type for product change
        assert "0006" in result  # Change option set
        assert "15" in result  # Cycle day
        assert "N" in result  # Card delivery

    def test_generate_nonmon_record_all_valid_change_option_sets(self, generator):
        """Test generate_nonmon_record with all valid change option sets"""
        for option_set in VALID_CHANGE_OPTION_SETS.keys():
            account_id = "2222222217"
            cycle_day = "15"
            original_row = [account_id, option_set, cycle_day]

            result = generator.generate_nonmon_record(account_id, original_row)

            assert len(result) == 596
            assert option_set in result
            assert "260" in result

    def test_generate_nonmon_record_cycle_day_single_digit(self, generator):
        """Test generate_nonmon_record with single digit cycle day (should zero-pad)"""
        account_id = "2222222217"
        change_option_set = "0006"
        cycle_day = "5"  # Single digit
        original_row = [account_id, change_option_set, cycle_day]

        result = generator.generate_nonmon_record(account_id, original_row)

        # Cycle day should be zero-padded to 2 digits
        assert "05" in result
        assert len(result) == 596

    def test_generate_nonmon_record_cycle_day_with_whitespace(self, generator):
        """Test generate_nonmon_record with whitespace in cycle day"""
        account_id = "2222222217"
        change_option_set = "0006"
        cycle_day = "  8  "  # Whitespace around single digit
        original_row = [account_id, change_option_set, cycle_day]

        result = generator.generate_nonmon_record(account_id, original_row)

        # Should strip and zero-pad
        assert "08" in result
        assert len(result) == 596

    def test_generate_nonmon_record_none_original_row_raises_error(self, generator):
        """Test that None original_row raises an AirflowFailException"""
        account_id = "2222222217"
        original_row = None

        with pytest.raises(AirflowFailException) as exc_info:
            generator.generate_nonmon_record(account_id, original_row)

        assert "The input row should contain: account_id, change_option_set, cycle_day." in str(exc_info.value)
        assert f"Account ID: {account_id}" in str(exc_info.value)
        assert "Row: None" in str(exc_info.value)

    def test_generate_nonmon_record_empty_list_raises_error(self, generator):
        """Test that empty list raises an AirflowFailException"""
        account_id = "2222222217"
        original_row = []

        with pytest.raises(AirflowFailException) as exc_info:
            generator.generate_nonmon_record(account_id, original_row)

        assert "The input row should contain: account_id, change_option_set, cycle_day." in str(exc_info.value)
        assert f"Account ID: {account_id}" in str(exc_info.value)
        assert "Row: []" in str(exc_info.value)

    def test_generate_nonmon_record_insufficient_data_raises_error(self, generator):
        """Test that insufficient data in row raises an AirflowFailException"""
        account_id = "2222222217"
        original_row = [account_id, "0006"]  # Missing cycle_day

        with pytest.raises(AirflowFailException) as exc_info:
            generator.generate_nonmon_record(account_id, original_row)

        assert "The input row should contain: account_id, change_option_set, cycle_day." in str(exc_info.value)
        assert f"Account ID: {account_id}" in str(exc_info.value)

    def test_generate_nonmon_record_non_list_original_row_raises_error(self, generator):
        """Test that non-list original_row raises an AirflowFailException"""
        account_id = "2222222217"
        original_row = "not_a_list"

        with pytest.raises(AirflowFailException) as exc_info:
            generator.generate_nonmon_record(account_id, original_row)

        assert "The input row should contain: account_id, change_option_set, cycle_day." in str(exc_info.value)
        assert f"Account ID: {account_id}" in str(exc_info.value)

    def test_generate_nonmon_record_invalid_change_option_set_raises_error(self, generator):
        """Test that invalid change_option_set raises an AirflowFailException"""
        account_id = "2222222217"
        change_option_set = "9999"  # Invalid option
        cycle_day = "15"
        original_row = [account_id, change_option_set, cycle_day]

        with pytest.raises(AirflowFailException) as exc_info:
            generator.generate_nonmon_record(account_id, original_row)

        assert "Invalid change_option_set: 9999" in str(exc_info.value)
        assert "Valid values:" in str(exc_info.value)
        assert f"Account ID: {account_id}" in str(exc_info.value)

    def test_generate_nonmon_record_change_option_set_with_whitespace(self, generator):
        """Test generate_nonmon_record with whitespace in change_option_set"""
        account_id = "2222222217"
        change_option_set = "  0006  "  # Whitespace
        cycle_day = "15"
        original_row = [account_id, change_option_set, cycle_day]

        result = generator.generate_nonmon_record(account_id, original_row)

        # Should strip whitespace and process correctly
        assert "0006" in result
        assert len(result) == 596

    def test_generate_nonmon_record_contains_fixed_fields(self, generator):
        """Test that record contains all fixed field values"""
        account_id = "2222222217"
        change_option_set = "0006"
        cycle_day = "15"
        original_row = [account_id, change_option_set, cycle_day]

        result = generator.generate_nonmon_record(account_id, original_row)

        # Verify fixed fields
        assert CARD_DELIVERY in result  # 'N'
        assert ALT_SHIPPING_ADDRESS_IND in result  # 'N'
        # PIN_MAILER_TYPE is a space, harder to verify directly

    def test_generate_nonmon_record_record_type_260(self, generator):
        """Test that record type is 260 for product change"""
        account_id = "2222222217"
        change_option_set = "0006"
        cycle_day = "15"
        original_row = [account_id, change_option_set, cycle_day]

        result = generator.generate_nonmon_record(account_id, original_row)

        # Record type should be 260
        assert "260" in result


class TestProductChangeFileGeneratorBuildNonmonTable:
    """Test cases for build_nonmon_table method"""

    def test_build_nonmon_table_generates_correct_ddl(self, generator):
        """Test build_nonmon_table generates correct DDL for product change"""
        table_id = 'test_project.test_dataset.product_change_table'
        result = generator.build_nonmon_table(table_id)

        # Verify DDL structure
        assert f"CREATE TABLE IF NOT EXISTS `{table_id}`" in result
        assert "ACCOUNT_ID STRING" in result
        # assert "FILLER STRING" in result
        assert "TSYS_ID STRING" in result
        assert "RECORD_TYPE STRING" in result
        assert "CHANGE_OPTION_SET STRING" in result
        assert "CYCLE_DAY STRING" in result
        assert "CARD_DELIVERY STRING" in result
        assert "PIN_MAILER_TYPE STRING" in result
        assert "ALT_SHIPPING_ADDRESS_IND STRING" in result
        assert "REC_LOAD_TIMESTAMP DATETIME" in result
        assert "FILE_NAME STRING" in result

    def test_build_nonmon_table_with_different_table_id(self, generator):
        """Test build_nonmon_table with different table IDs"""
        table_ids = [
            'project1.dataset1.table1',
            'project2.dataset2.table2',
            'pcb-prod-curated.domain_operations.NONMON_PRODUCT_CHANGE'
        ]

        for table_id in table_ids:
            result = generator.build_nonmon_table(table_id)
            assert f"CREATE TABLE IF NOT EXISTS `{table_id}`" in result


class TestProductChangeFileGeneratorBuildNonmonQuery:
    """Test cases for build_nonmon_query method"""

    def test_build_nonmon_query_single_view(self, generator):
        """Test build_nonmon_query with a single view"""
        transformed_views = [
            {
                'id': 'test_project.test_dataset.product_change_view',
                'columns': 'record_data, file_name'
            }
        ]
        table_id = 'test_project.test_dataset.product_change_table'

        result = generator.build_nonmon_query(transformed_views, table_id)

        # Verify query structure
        assert f"INSERT INTO {table_id}" in result
        assert "SUBSTR(record_data, 1, 19)   AS ACCOUNT_ID" in result
        # assert "SUBSTR(record_data, 20, 3)   AS FILLER" in result
        assert "SUBSTR(record_data, 20, 6)   AS TSYS_ID" in result
        assert "SUBSTR(record_data, 26, 3)   AS RECORD_TYPE" in result
        assert "SUBSTR(record_data, 35, 4)   AS CHANGE_OPTION_SET" in result
        assert "SUBSTR(record_data, 45, 2)   AS CYCLE_DAY" in result
        assert "SUBSTR(record_data, 53, 1)   AS CARD_DELIVERY" in result
        assert "SUBSTR(record_data, 60, 1)   AS PIN_MAILER_TYPE" in result
        assert "SUBSTR(record_data, 67, 1)   AS ALT_SHIPPING_ADDRESS_IND" in result
        assert "CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP" in result
        assert "file_name AS FILE_NAME" in result
        assert "`test_project.test_dataset.product_change_view`" in result

    def test_build_nonmon_query_multiple_views(self, generator):
        """Test build_nonmon_query with multiple views"""
        transformed_views = [
            {
                'id': 'test_project.test_dataset.product_change_view1',
                'columns': 'record_data, file_name'
            },
            {
                'id': 'test_project.test_dataset.product_change_view2',
                'columns': 'record_data, file_name'
            },
            {
                'id': 'test_project.test_dataset.product_change_view3',
                'columns': 'record_data, file_name'
            }
        ]
        table_id = 'test_project.test_dataset.product_change_table'

        result = generator.build_nonmon_query(transformed_views, table_id)

        # Verify UNION ALL structure
        assert f"INSERT INTO {table_id}" in result
        assert result.count("UNION ALL") == 2  # Two UNION ALLs for three views
        assert "`test_project.test_dataset.product_change_view1`" in result
        assert "`test_project.test_dataset.product_change_view2`" in result
        assert "`test_project.test_dataset.product_change_view3`" in result
        assert result.count("SELECT") == 3  # Three SELECT statements

    def test_build_nonmon_query_filters_header_trailer(self, generator):
        """Test that build_nonmon_query includes WHERE clause to filter header/trailer records"""
        transformed_views = [
            {
                'id': 'test_project.test_dataset.product_change_view',
                'columns': 'record_data, file_name'
            }
        ]
        table_id = 'test_project.test_dataset.product_change_table'

        result = generator.build_nonmon_query(transformed_views, table_id)

        # Verify WHERE clause filters header and trailer records
        assert "WHERE NOT REGEXP_CONTAINS(record_data, r'" in result
        assert HEADER_IDNTFR in result
        assert TRAILER_IDNTFR in result

    def test_build_nonmon_query_empty_views_list(self, generator):
        """Test build_nonmon_query with empty views list"""
        transformed_views = []
        table_id = 'test_project.test_dataset.product_change_table'

        result = generator.build_nonmon_query(transformed_views, table_id)

        # Should still have INSERT INTO but empty content
        assert f"INSERT INTO {table_id}" in result
        assert "UNION ALL" not in result  # No UNION ALL with empty list

    def test_build_nonmon_query_with_different_column_names(self, generator):
        """Test build_nonmon_query handles different column names correctly"""
        transformed_views = [
            {
                'id': 'test_project.test_dataset.product_change_view',
                'columns': 'different_record_col, different_file_col'
            }
        ]
        table_id = 'test_project.test_dataset.product_change_table'

        result = generator.build_nonmon_query(transformed_views, table_id)

        # Should use the specified column names
        assert "REGEXP_CONTAINS(different_record_col," in result
        assert "different_file_col AS FILE_NAME" in result

    @patch('pcb_ops_processing.nonmon.product_change_generator.logging.getLogger')
    def test_build_nonmon_query_logs_debug_info(self, mock_get_logger, generator):
        """Test that build_nonmon_query logs appropriate debug information"""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        transformed_views = [
            {
                'id': 'test_project.test_dataset.product_change_view',
                'columns': 'record_data, file_name'
            }
        ]
        table_id = 'test_project.test_dataset.product_change_table'

        generator.build_nonmon_query(transformed_views, table_id)

        # Verify logging calls
        assert mock_logger.info.call_count >= 2  # At least 2 info calls


class TestProductChangeFileGeneratorConstants:
    """Test cases for module constants"""

    def test_valid_change_option_sets_contains_all_required_options(self):
        """Test that VALID_CHANGE_OPTION_SETS contains all required card types"""
        expected_options = {
            '0004': 'PCB WORLD CHIP/PIN',
            '0005': 'PCB WORLD CHIP/SIG',
            '0006': 'PCB SILVER CHIP/PIN',
            '0007': 'PCB SILVER CHIP/SIG',
        }

        assert VALID_CHANGE_OPTION_SETS == expected_options

    def test_card_delivery_is_n(self):
        """Test that CARD_DELIVERY is 'N'"""
        assert CARD_DELIVERY == 'N'

    def test_pin_mailer_type_is_space(self):
        """Test that PIN_MAILER_TYPE is a space (null)"""
        assert PIN_MAILER_TYPE == 'N'

    def test_alt_shipping_address_ind_is_n(self):
        """Test that ALT_SHIPPING_ADDRESS_IND is 'N'"""
        assert ALT_SHIPPING_ADDRESS_IND == 'N'


class TestProductChangeFileGeneratorEdgeCases:
    """Test edge cases for ProductChangeFileGenerator"""

    def test_generate_nonmon_record_with_very_long_account_id(self, generator):
        """Test generate_nonmon_record with account ID longer than expected"""
        account_id = "12345678901234567890"  # 20 digits
        change_option_set = "0006"
        cycle_day = "15"
        original_row = [account_id, change_option_set, cycle_day]

        result = generator.generate_nonmon_record(account_id, original_row)

        # Should still produce a 596-char record
        assert len(result) == 596

    def test_generate_nonmon_record_with_numeric_cycle_day(self, generator):
        """Test generate_nonmon_record when cycle_day is numeric"""
        account_id = "2222222217"
        change_option_set = "0006"
        cycle_day = 15  # Numeric instead of string
        original_row = [account_id, change_option_set, cycle_day]

        result = generator.generate_nonmon_record(account_id, original_row)

        assert "15" in result
        assert len(result) == 596

    def test_generate_nonmon_record_cycle_day_zero(self, generator):
        """Test generate_nonmon_record with cycle day of 0"""
        account_id = "2222222217"
        change_option_set = "0006"
        cycle_day = "0"
        original_row = [account_id, change_option_set, cycle_day]

        result = generator.generate_nonmon_record(account_id, original_row)

        # Should zero-pad to "00"
        assert len(result) == 596

    def test_generate_nonmon_record_max_cycle_day(self, generator):
        """Test generate_nonmon_record with max cycle day (31)"""
        account_id = "2222222217"
        change_option_set = "0006"
        cycle_day = "31"
        original_row = [account_id, change_option_set, cycle_day]

        result = generator.generate_nonmon_record(account_id, original_row)

        assert "31" in result
        assert len(result) == 596

    def test_generate_nonmon_record_all_card_types(self, generator):
        """Test generate_nonmon_record with each valid card type"""
        test_cases = [
            ('0004', 'PCB WORLD CHIP/PIN'),
            ('0005', 'PCB WORLD CHIP/SIG'),
            ('0006', 'PCB SILVER CHIP/PIN'),
            ('0007', 'PCB SILVER CHIP/SIG'),
        ]

        for change_option_set, _ in test_cases:
            account_id = "2222222217"
            cycle_day = "15"
            original_row = [account_id, change_option_set, cycle_day]

            result = generator.generate_nonmon_record(account_id, original_row)

            assert len(result) == 596
            assert change_option_set in result

    def test_generate_nonmon_record_dict_original_row_raises_error(self, generator):
        """Test that dict original_row raises an AirflowFailException"""
        account_id = "2222222217"
        original_row = {'account_id': '2222222217', 'change_option_set': '0006', 'cycle_day': '15'}

        with pytest.raises(AirflowFailException) as exc_info:
            generator.generate_nonmon_record(account_id, original_row)

        assert "The input row should contain: account_id, change_option_set, cycle_day." in str(exc_info.value)

    def test_generate_nonmon_record_tuple_original_row_raises_error(self, generator):
        """Test that tuple original_row raises an AirflowFailException (not a list)"""
        account_id = "2222222217"
        original_row = ("2222222217", "0006", "15")  # Tuple instead of list

        with pytest.raises(AirflowFailException) as exc_info:
            generator.generate_nonmon_record(account_id, original_row)

        assert "The input row should contain: account_id, change_option_set, cycle_day." in str(exc_info.value)


class TestProductChangeFileGeneratorRecordLayout:
    """Test cases to verify the exact record layout"""

    def test_record_layout_positions(self, generator):
        """Test that record fields are in correct positions"""
        account_id = "2222222217"
        change_option_set = "0006"
        cycle_day = "15"
        original_row = [account_id, change_option_set, cycle_day]

        result = generator.generate_nonmon_record(account_id, original_row)

        # Verify field positions (0-indexed)
        # Position 1-19: Account ID (IIIIIII222222221700)
        assert result[0:19] == "IIIIIII222222221700"

        # Position 20-22: Filler (@@@)
        # assert result[19:22] == "@@@"

        # Position 23-28: TSYS_ID (6 chars, left-justified)
        # The actual TSYS_ID value depends on the constant

        # Position 26-28: Record Type (260)
        assert result[25:28] == "260"

        # Position 35-38: Change Option Set (0006)
        assert result[34:38] == "0006"

        # Position 45-46: Cycle Day (15)
        assert result[44:46] == "15"

        # Position 53: Card Delivery (N)
        assert result[52] == "N"

        # Position 60: PIN Mailer Type (space)
        assert result[59] == "N"

        # Position 67: Alt Shipping Address Ind (N)
        assert result[66] == "N"

    def test_record_total_length(self, generator):
        """Test that record is exactly 596 characters"""
        account_id = "2222222217"
        change_option_set = "0006"
        cycle_day = "15"
        original_row = [account_id, change_option_set, cycle_day]

        result = generator.generate_nonmon_record(account_id, original_row)

        assert len(result) == 596

    def test_record_padding_is_spaces(self, generator):
        """Test that record padding (after field 40) is spaces"""
        account_id = "2222222217"
        change_option_set = "0006"
        cycle_day = "15"
        original_row = [account_id, change_option_set, cycle_day]

        result = generator.generate_nonmon_record(account_id, original_row)

        # After position 40, rest should be spaces
        padding = result[221:]
        assert padding == " " * (596 - 221)
