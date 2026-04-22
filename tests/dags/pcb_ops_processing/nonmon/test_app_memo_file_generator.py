import pytest
from airflow.exceptions import AirflowFailException
from unittest.mock import patch, MagicMock
import logging

from pcb_ops_processing.nonmon.app_memo_file_generator import AppMemoFileGenerator
import util.constants as consts


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
        'dag_id': 'test_memo_dag',
        'default_args': {
            'owner': 'test_owner',
            'retries': 1
        }
    }


class TestAppMemoFileGenerator:
    """Test cases for AppMemoFileGenerator class"""

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_generate_nonmon_record_valid_input(self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config,
                                                mock_config):
        """Test generate_nonmon_record with valid input"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = AppMemoFileGenerator('test_config.yaml')

        # Test with valid memo texts
        memo_texts = ['First memo', 'Second memo', 'Third memo']
        account_id = 12345
        original_row = [str(account_id)] + memo_texts

        result = generator.generate_nonmon_record(account_id, original_row)

        # Verify the record structure
        assert len(result) == 596  # Standard record length
        assert result.startswith("IIIIII00000012345")  # Header and account ID
        assert "SDDATA910" in result  # Data identifier
        assert "953179First memo" in result  # First memo with ID
        assert "953279Second memo" in result  # Second memo with ID
        assert "953379Third memo" in result  # Third memo with ID

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_generate_nonmon_record_with_empty_memos(self, mock_settings, mock_read_yaml, mock_read_var,
                                                     mock_gcp_config, mock_config):
        """Test generate_nonmon_record with some empty memo texts"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = AppMemoFileGenerator('test_config.yaml')

        # Test with some empty memo texts
        memo_texts = ['First memo', '', 'Third memo']
        account_id = 98765
        original_row = [str(account_id)] + memo_texts

        result = generator.generate_nonmon_record(account_id, original_row)

        # Verify the record structure
        assert len(result) == 596
        assert result.startswith("IIIIII00000098765")
        assert "953179First memo" in result
        assert "953279" in result  # Second memo ID with empty content
        assert "953379Third memo" in result

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_generate_nonmon_record_truncates_long_memos(self, mock_settings, mock_read_yaml, mock_read_var,
                                                         mock_gcp_config, mock_config):
        """Test that long memo texts are truncated to 79 characters"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = AppMemoFileGenerator('test_config.yaml')

        # Test with a very long memo text (more than 79 characters)
        long_memo = 'A' * 100  # 100 characters
        memo_texts = [long_memo]
        account_id = 11111
        original_row = [str(account_id)] + memo_texts

        result = generator.generate_nonmon_record(account_id, original_row)

        # The memo should be truncated to 79 characters
        expected_memo = 'A' * 79
        assert f"953179{expected_memo}" in result

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_generate_nonmon_record_too_many_memos_raises_error(self, mock_settings, mock_read_yaml, mock_read_var,
                                                                mock_gcp_config, mock_config):
        """Test that providing more than 5 memo texts raises a ValueError"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = AppMemoFileGenerator('test_config.yaml')

        # Test with more than 5 memo texts
        memo_texts = ['Memo1', 'Memo2', 'Memo3', 'Memo4', 'Memo5', 'Memo6']
        account_id = 22222
        original_row = [str(account_id)] + memo_texts

        with pytest.raises(AirflowFailException) as exc_info:
            generator.generate_nonmon_record(account_id, original_row)

        assert "Too many memo texts provided (6). Maximum allowed is 5." in str(exc_info.value)
        assert "Account ID: 22222" in str(exc_info.value)

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_generate_nonmon_record_with_original_row_in_error(self, mock_settings, mock_read_yaml, mock_read_var,
                                                               mock_gcp_config, mock_config):
        """Test that original row info is included in error messages"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = AppMemoFileGenerator('test_config.yaml')

        memo_texts = ['Memo1', 'Memo2', 'Memo3', 'Memo4', 'Memo5', 'Memo6']
        account_id = 33333
        original_row = [str(account_id)] + memo_texts

        with pytest.raises(AirflowFailException) as exc_info:
            generator.generate_nonmon_record(account_id, original_row)

        assert f"Row: {original_row}" in str(exc_info.value)

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_build_nonmon_table(self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config, mock_config):
        """Test build_nonmon_table generates correct DDL"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = AppMemoFileGenerator('test_config.yaml')

        table_id = 'test_project.test_dataset.memo_table'
        result = generator.build_nonmon_table(table_id)

        # Verify DDL structure
        assert f"CREATE TABLE IF NOT EXISTS `{table_id}`" in result
        assert "FILLER_1 STRING" in result
        assert "CIFP_ACCOUNT_ID STRING" in result
        assert "TSYSID STRING" in result
        assert "RECORD_TYPE STRING" in result
        assert "MEMO_1_ID STRING" in result
        assert "MEMO_1 STRING" in result
        assert "MEMO_5_ID STRING" in result
        assert "MEMO_5 STRING" in result
        assert "REC_LOAD_TIMESTAMP DATETIME" in result
        assert "FILE_NAME STRING" in result

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_build_nonmon_query_single_view(self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config,
                                            mock_config):
        """Test build_nonmon_query with a single view"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = AppMemoFileGenerator('test_config.yaml')

        transformed_views = [
            {
                'id': 'test_project.test_dataset.memo_view',
                'columns': 'record_data, file_name'
            }
        ]
        table_id = 'test_project.test_dataset.memo_table'

        result = generator.build_nonmon_query(transformed_views, table_id)

        # Verify query structure
        assert f"INSERT INTO {table_id}" in result
        assert "SUBSTR(record_data, 1, 6)    AS FILLER_1" in result
        assert "SUBSTR(record_data, 7, 11)   AS CIFP_ACCOUNT_ID" in result
        assert "SUBSTR(record_data, 35, 79)  AS MEMO_1" in result
        assert "SUBSTR(record_data, 375, 79) AS MEMO_5" in result
        assert "CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP" in result
        assert "file_name" in result
        assert "`test_project.test_dataset.memo_view`" in result

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_build_nonmon_query_multiple_views(self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config,
                                               mock_config):
        """Test build_nonmon_query with multiple views"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = AppMemoFileGenerator('test_config.yaml')

        transformed_views = [
            {
                'id': 'test_project.test_dataset.memo_view1',
                'columns': 'record_data, file_name'
            },
            {
                'id': 'test_project.test_dataset.memo_view2',
                'columns': 'record_data, file_name'
            }
        ]
        table_id = 'test_project.test_dataset.memo_table'

        result = generator.build_nonmon_query(transformed_views, table_id)

        # Verify UNION ALL structure
        assert f"INSERT INTO {table_id}" in result
        assert "UNION ALL" in result
        assert "`test_project.test_dataset.memo_view1`" in result
        assert "`test_project.test_dataset.memo_view2`" in result
        assert result.count("SELECT") == 2  # Two SELECT statements

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    @patch('pcb_ops_processing.nonmon.app_memo_file_generator.logging.getLogger')
    def test_build_nonmon_query_logs_debug_info(self, mock_get_logger, mock_settings, mock_read_yaml, mock_read_var,
                                                mock_gcp_config, mock_config):
        """Test that build_nonmon_query logs appropriate debug information"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        generator = AppMemoFileGenerator('test_config.yaml')

        transformed_views = [
            {
                'id': 'test_project.test_dataset.memo_view',
                'columns': 'record_data, file_name'
            }
        ]
        table_id = 'test_project.test_dataset.memo_table'

        generator.build_nonmon_query(transformed_views, table_id)

        # Verify logging calls
        assert mock_logger.info.call_count >= 2  # At least 4 info calls

        # add flexible content checks for the two multi-line messages
        logged_msgs = [c.args[0] for c in mock_logger.info.call_args_list]
        assert any("Building query for view: test_project.test_dataset.memo_view" in m for m in logged_msgs)
        assert any("record   :: record_data" in m for m in logged_msgs)
        assert any("filename :: file_name" in m for m in logged_msgs)
        assert any("Final combined query for memo" in m for m in logged_msgs)


class TestAppMemoFileGeneratorEdgeCases:
    """Test edge cases for AppMemoFileGenerator"""

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_generate_nonmon_record_with_empty_list(self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config,
                                                    mock_config):
        """Test generate_nonmon_record with empty memo list"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = AppMemoFileGenerator('test_config.yaml')

        memo_texts = []
        account_id = 55555
        original_row = [str(account_id)] + memo_texts

        result = generator.generate_nonmon_record(account_id, original_row)

        # Should pad with empty strings
        assert len(result) == 596
        assert result.startswith("IIIIII00000055555")
        # All memo fields should be empty (just the IDs with 79 spaces each)
        assert "953179" + " " * 79 in result
        assert "953579" + " " * 79 in result

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_generate_nonmon_record_with_exactly_five_memos(self, mock_settings, mock_read_yaml, mock_read_var,
                                                            mock_gcp_config, mock_config):
        """Test generate_nonmon_record with exactly 5 memo texts (boundary case)"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = AppMemoFileGenerator('test_config.yaml')

        memo_texts = ['Memo1', 'Memo2', 'Memo3', 'Memo4', 'Memo5']
        account_id = 66666
        original_row = [str(account_id)] + memo_texts

        result = generator.generate_nonmon_record(account_id, original_row)

        # Should work without error
        assert len(result) == 596
        assert "953179Memo1" in result
        assert "953579Memo5" in result

    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env')
    @patch('pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings')
    def test_build_nonmon_query_empty_views_list(self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config,
                                                 mock_config):
        """Test build_nonmon_query with empty views list"""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = '/test/dags'

        generator = AppMemoFileGenerator('test_config.yaml')

        transformed_views = []
        table_id = 'test_project.test_dataset.memo_table'

        result = generator.build_nonmon_query(transformed_views, table_id)

        # Should still have INSERT INTO but empty content
        assert f"INSERT INTO {table_id}" in result
        assert "UNION ALL" not in result  # No UNION ALL with empty list
