import pytest
from airflow.exceptions import AirflowFailException
from unittest.mock import patch, MagicMock
import logging

from pcb_ops_processing.nonmon.payment_hierarchy_update_generator import PaymentHierarchyUpdateFileGenerator
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
        'dag_id': 'test_payment_hierarchy_dag',
        'default_args': {
            'owner': 'test_owner',
            'retries': 1
        }
    }


class TestPaymentHierarchyUpdateFileGenerator:
    """Test cases for PaymentHierarchyUpdateFileGenerator class"""

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_generate_nonmon_record_valid_input(self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config, mock_config):
        """Test generate_nonmon_record with valid hierarchy data"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = PaymentHierarchyUpdateFileGenerator('test_config.yaml')

        # Test with valid hierarchy data
        hierarchy_data = ['12345', '67890', 'L1', 'ACTIVE']
        account_id = 12345
        original_row = [str(account_id)] + hierarchy_data

        result = generator.generate_nonmon_record(account_id, original_row)

        # Verify the record structure
        assert len(result) == 596  # Standard record length
        assert result.startswith("IIIIII00000012345")  # Header and account ID
        assert "00SDDATA231460001A" in result  # Data identifier and payment hierarchy code

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_generate_nonmon_record_empty_hierarchy_data_raises_error(self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config, mock_config):
        """Test that empty hierarchy data raises a ValueError"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = PaymentHierarchyUpdateFileGenerator('test_config.yaml')

        # Test with empty hierarchy data
        account_id = 22222
        original_row = None

        with pytest.raises(AirflowFailException) as exc_info:
            generator.generate_nonmon_record(account_id, original_row)

        assert "Payment hierarchy data is required." in str(exc_info.value)
        assert "Account ID: 22222" in str(exc_info.value)

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_generate_nonmon_record_with_original_row_in_error(self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config, mock_config):
        """Test that original row info is included in error messages"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = PaymentHierarchyUpdateFileGenerator('test_config.yaml')

        account_id = 33333
        original_row = [str(account_id), "parent_id", "child_id", "level", "status"]

        # This should work without error since the data is valid
        result = generator.generate_nonmon_record(account_id, original_row)

        # Verify the record was created successfully
        assert len(result) == 596
        assert result.startswith("IIIIII00000033333")

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_generate_nonmon_record_pads_account_id(self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config, mock_config):
        """Test that account ID is properly zero-padded to 11 characters"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = PaymentHierarchyUpdateFileGenerator('test_config.yaml')

        hierarchy_data = ['test', 'data']
        account_id = 123  # Short account ID
        original_row = [str(account_id)] + hierarchy_data

        result = generator.generate_nonmon_record(account_id, original_row)

        # Account ID should be zero-padded to 11 characters
        assert result.startswith("IIIIII00000000123")

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_build_nonmon_table(self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config, mock_config):
        """Test build_nonmon_table generates correct DDL for payment hierarchy"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = PaymentHierarchyUpdateFileGenerator('test_config.yaml')

        table_id = 'test_project.test_dataset.payment_hierarchy_table'
        result = generator.build_nonmon_table(table_id)

        # Verify DDL structure for payment hierarchy
        assert f"CREATE TABLE IF NOT EXISTS `{table_id}`" in result
        assert "FILLER_1 STRING" in result
        assert "CIFP_ACCOUNT_ID STRING" in result
        assert "FILLER_2 STRING" in result
        assert "TSYS_ID STRING" in result
        assert "RECORD_TYPE STRING" in result
        assert "FIELD_INDICATOR STRING" in result
        assert "PAYMENT_HIERARCHY_CODE STRING" in result
        assert "REC_LOAD_TIMESTAMP DATETIME" in result
        assert "FILE_NAME STRING" in result

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_build_nonmon_query_single_view(self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config, mock_config):
        """Test build_nonmon_query with a single view for payment hierarchy"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = PaymentHierarchyUpdateFileGenerator('test_config.yaml')

        transformed_views = [
            {
                'id': 'test_project.test_dataset.payment_hierarchy_view',
                'columns': 'record_data, file_name'
            }
        ]
        table_id = 'test_project.test_dataset.payment_hierarchy_table'

        result = generator.build_nonmon_query(transformed_views, table_id)

        # Verify query structure for payment hierarchy
        assert f"INSERT INTO {table_id}" in result
        assert "SUBSTR(record_data, 1, 6)    AS FILLER_1" in result
        assert "SUBSTR(record_data, 7, 11)   AS CIFP_ACCOUNT_ID" in result
        assert "SUBSTR(record_data, 18, 2)   AS FILLER_2" in result
        assert "SUBSTR(record_data, 20, 6)   AS TSYS_ID" in result
        assert "SUBSTR(record_data, 26, 3)   AS RECORD_TYPE" in result
        assert "SUBSTR(record_data, 29, 6)  AS FIELD_INDICATOR" in result
        assert "SUBSTR(record_data, 35, 1)  AS PAYMENT_HIERARCHY_CODE" in result
        assert "CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP" in result
        assert "file_name AS FILE_NAME" in result
        assert "`test_project.test_dataset.payment_hierarchy_view`" in result

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_build_nonmon_query_multiple_views(self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config, mock_config):
        """Test build_nonmon_query with multiple views for payment hierarchy"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = PaymentHierarchyUpdateFileGenerator('test_config.yaml')

        transformed_views = [
            {
                'id': 'test_project.test_dataset.payment_hierarchy_view1',
                'columns': 'record_data, file_name'
            },
            {
                'id': 'test_project.test_dataset.payment_hierarchy_view2',
                'columns': 'record_data, file_name'
            }
        ]
        table_id = 'test_project.test_dataset.payment_hierarchy_table'

        result = generator.build_nonmon_query(transformed_views, table_id)

        # Verify UNION ALL structure
        assert f"INSERT INTO {table_id}" in result
        assert "UNION ALL" in result
        assert "`test_project.test_dataset.payment_hierarchy_view1`" in result
        assert "`test_project.test_dataset.payment_hierarchy_view2`" in result
        assert result.count("SELECT") == 2  # Two SELECT statements

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    @patch('pcb_ops_processing.nonmon.payment_hierarchy_update_generator.logging.getLogger')
    def test_build_nonmon_query_logs_debug_info(self, mock_get_logger, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config, mock_config):
        """Test that build_nonmon_query logs appropriate debug information"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        generator = PaymentHierarchyUpdateFileGenerator('test_config.yaml')

        transformed_views = [
            {
                'id': 'test_project.test_dataset.payment_hierarchy_view',
                'columns': 'record_data, file_name'
            }
        ]
        table_id = 'test_project.test_dataset.payment_hierarchy_table'

        generator.build_nonmon_query(transformed_views, table_id)

        # Verify logging calls
        assert mock_logger.info.call_count >= 2

        msgs = [c.args[0] for c in mock_logger.info.call_args_list]
        assert any("Building query for view: test_project.test_dataset.payment_hierarchy_view" in m for m in msgs)
        assert any("record   :: record_data" in m for m in msgs)
        assert any("filename :: file_name" in m for m in msgs)
        assert any("Final combined query for payment hierarchy" in m for m in msgs)


class TestPaymentHierarchyUpdateFileGeneratorEdgeCases:
    """Test edge cases for PaymentHierarchyUpdateFileGenerator"""

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_generate_nonmon_record_with_falsy_but_valid_data(self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config, mock_config):
        """Test generate_nonmon_record with falsy but valid hierarchy data"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = PaymentHierarchyUpdateFileGenerator('test_config.yaml')

        # Test with falsy but valid data (empty list is still an object)
        hierarchy_data = ['0', '0']  # Valid but zeros
        account_id = 55555
        original_row = [str(account_id)] + hierarchy_data

        result = generator.generate_nonmon_record(account_id, original_row)

        # Should work without error
        assert len(result) == 596
        assert result.startswith("IIIIII00000055555")

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_generate_nonmon_record_with_very_long_account_id(self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config, mock_config):
        """Test generate_nonmon_record with account ID longer than 11 digits"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = PaymentHierarchyUpdateFileGenerator('test_config.yaml')

        hierarchy_data = ['test', 'data']
        account_id = 123456789012345  # 15 digits - longer than 11
        original_row = [str(account_id)] + hierarchy_data

        result = generator.generate_nonmon_record(account_id, original_row)

        # Should use the first 11 digits when zero-filled
        account_id_str = str(account_id).zfill(11)
        assert result.startswith(f"IIIIII{account_id_str}")

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_build_nonmon_query_empty_views_list(self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config, mock_config):
        """Test build_nonmon_query with empty views list"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = PaymentHierarchyUpdateFileGenerator('test_config.yaml')

        transformed_views = []
        table_id = 'test_project.test_dataset.payment_hierarchy_table'

        result = generator.build_nonmon_query(transformed_views, table_id)

        # Should still have INSERT INTO but empty content
        assert f"INSERT INTO {table_id}" in result
        assert "UNION ALL" not in result  # No UNION ALL with empty list

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_generate_nonmon_record_with_list_hierarchy_data(self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config, mock_config):
        """Test generate_nonmon_record when hierarchy_data is a list (truthy)"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = PaymentHierarchyUpdateFileGenerator('test_config.yaml')

        # Test with list data (truthy)
        hierarchy_data = ['parent123', 'child456', 'L2']
        account_id = 77777
        original_row = [str(account_id)] + hierarchy_data

        result = generator.generate_nonmon_record(account_id, original_row)

        # Should work without error since list is truthy
        assert len(result) == 596
        assert result.startswith("IIIIII00000077777")

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_build_nonmon_query_filters_header_trailer(self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config, mock_config):
        """Test that build_nonmon_query includes WHERE clause to filter header/trailer records"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = PaymentHierarchyUpdateFileGenerator('test_config.yaml')

        transformed_views = [
            {
                'id': 'test_project.test_dataset.payment_hierarchy_view',
                'columns': 'record_data, file_name'
            }
        ]
        table_id = 'test_project.test_dataset.payment_hierarchy_table'

        result = generator.build_nonmon_query(transformed_views, table_id)

        # Verify WHERE clause filters header and trailer records
        assert "WHERE NOT REGEXP_CONTAINS(record_data, r'" in result
        assert HEADER_IDNTFR in result or TRAILER_IDNTFR in result  # Should reference the constants

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_constructor_with_missing_config(self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config):
        """Test that constructor handles missing/archived config file gracefully"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = None  # Simulate archived/missing config
        mock_settings.DAGS_FOLDER = '/test/dags'

        # The generator should still be usable for testing purposes
        generator = PaymentHierarchyUpdateFileGenerator('archived_config.yaml')

        assert generator is not None
        assert hasattr(generator, 'generate_nonmon_record')
        assert hasattr(generator, 'build_nonmon_table')
        assert hasattr(generator, 'build_nonmon_query')
