"""
Unit tests for reversal_search_tool.reversal_search_tool module.

Tests the main DAG task function with mocked BigQuery and Airflow context.
"""

from unittest.mock import MagicMock, patch, Mock
import pytest
from airflow.exceptions import AirflowException

from reversal_search_tool.reversal_search_tool import reversal_search_task


class TestReversalSearchTask:
    """Tests for reversal_search_task function."""

    @pytest.fixture
    def mock_context(self):
        """Create mock Airflow context."""
        context = {
            'dag_run': MagicMock()
        }
        return context

    @pytest.fixture
    def mock_bq_result_row(self):
        """Create mock BigQuery result row."""
        row = MagicMock()
        row.get = Mock(side_effect=lambda key: {
            'AccountUID': 1234567,
            'AccountID': 'ACC123456',
            'CustomerUID': 7654321,
            'CustomerID': 'CUST123456',
            'userID': 'USER123456',
            'ApplicationID': 9876543210
        }.get(key))
        return row

    @patch('reversal_search_tool.reversal_search_tool.read_variable_or_file')
    @patch('reversal_search_tool.reversal_search_tool.bigquery.Client')
    def test_reversal_search_task_email_success(
        self, mock_bq_client, mock_read_config, mock_context, mock_bq_result_row
    ):
        """Test successful reversal search with email input."""
        # Setup mocks
        mock_read_config.return_value = {'curated_zone_project_id': 'test-project'}
        mock_context['dag_run'].conf = {
            'input_type': 'email',
            'email': 'test@example.com'
        }

        mock_query_result = MagicMock()
        mock_query_result.total_rows = 1
        mock_query_result.__iter__ = Mock(return_value=iter([mock_bq_result_row]))

        mock_client_instance = MagicMock()
        mock_client_instance.query_and_wait.return_value = mock_query_result
        mock_bq_client.return_value = mock_client_instance

        # Execute
        result = reversal_search_task(**mock_context)

        # Assertions
        assert "Search completed successfully" in result
        assert "Found 1 record(s)" in result
        mock_client_instance.query_and_wait.assert_called_once()

    @patch('reversal_search_tool.reversal_search_tool.read_variable_or_file')
    def test_reversal_search_task_missing_project_id(
        self, mock_read_config, mock_context
    ):
        """Test error when project ID is missing."""
        mock_read_config.return_value = {}
        mock_context['dag_run'].conf = {
            'input_type': 'email',
            'email': 'test@example.com'
        }

        with pytest.raises(AirflowException, match="Missing curated_zone_project_id"):
            reversal_search_task(**mock_context)

    @patch('reversal_search_tool.reversal_search_tool.read_variable_or_file')
    def test_reversal_search_task_no_config(self, mock_read_config, mock_context):
        """Test error when no DAG config provided."""
        mock_read_config.return_value = {'curated_zone_project_id': 'test-project'}
        mock_context['dag_run'].conf = None

        with pytest.raises(AirflowException, match="No configuration provided"):
            reversal_search_task(**mock_context)

    @patch('reversal_search_tool.reversal_search_tool.read_variable_or_file')
    def test_reversal_search_task_empty_config(self, mock_read_config, mock_context):
        """Test error when DAG config is empty."""
        mock_read_config.return_value = {'curated_zone_project_id': 'test-project'}
        mock_context['dag_run'].conf = {}

        with pytest.raises(AirflowException, match="No configuration provided"):
            reversal_search_task(**mock_context)

    @patch('reversal_search_tool.reversal_search_tool.read_variable_or_file')
    def test_reversal_search_task_missing_input_type(
        self, mock_read_config, mock_context
    ):
        """Test error when input_type is missing."""
        mock_read_config.return_value = {'curated_zone_project_id': 'test-project'}
        mock_context['dag_run'].conf = {'email': 'test@example.com'}

        with pytest.raises(AirflowException, match="Missing required field: 'input_type'"):
            reversal_search_task(**mock_context)

    @patch('reversal_search_tool.reversal_search_tool.read_variable_or_file')
    def test_reversal_search_task_unsupported_input_type(
        self, mock_read_config, mock_context
    ):
        """Test error when input_type is unsupported."""
        mock_read_config.return_value = {'curated_zone_project_id': 'test-project'}
        mock_context['dag_run'].conf = {
            'input_type': 'invalid_type',
            'email': 'test@example.com'
        }

        with pytest.raises(AirflowException, match="Unsupported input_type"):
            reversal_search_task(**mock_context)

    @patch('reversal_search_tool.reversal_search_tool.read_variable_or_file')
    def test_reversal_search_task_validation_error(
        self, mock_read_config, mock_context
    ):
        """Test error when input validation fails."""
        mock_read_config.return_value = {'curated_zone_project_id': 'test-project'}
        mock_context['dag_run'].conf = {
            'input_type': 'email'
            # Missing 'email' field
        }

        with pytest.raises(AirflowException, match="Input validation failed"):
            reversal_search_task(**mock_context)

    @patch('reversal_search_tool.reversal_search_tool.read_variable_or_file')
    @patch('reversal_search_tool.reversal_search_tool.bigquery.Client')
    def test_reversal_search_task_bigquery_error(
        self, mock_bq_client, mock_read_config, mock_context
    ):
        """Test error when BigQuery query fails."""
        mock_read_config.return_value = {'curated_zone_project_id': 'test-project'}
        mock_context['dag_run'].conf = {
            'input_type': 'email',
            'email': 'test@example.com'
        }

        mock_client_instance = MagicMock()
        mock_client_instance.query_and_wait.side_effect = Exception("BigQuery error")
        mock_bq_client.return_value = mock_client_instance

        with pytest.raises(AirflowException, match="BigQuery query execution failed"):
            reversal_search_task(**mock_context)

    @patch('reversal_search_tool.reversal_search_tool.read_variable_or_file')
    @patch('reversal_search_tool.reversal_search_tool.bigquery.Client')
    def test_reversal_search_task_no_results(
        self, mock_bq_client, mock_read_config, mock_context
    ):
        """Test reversal search with no results."""
        mock_read_config.return_value = {'curated_zone_project_id': 'test-project'}
        mock_context['dag_run'].conf = {
            'input_type': 'email',
            'email': 'test@example.com'
        }

        mock_query_result = MagicMock()
        mock_query_result.total_rows = 0
        mock_query_result.__iter__ = Mock(return_value=iter([]))

        mock_client_instance = MagicMock()
        mock_client_instance.query_and_wait.return_value = mock_query_result
        mock_bq_client.return_value = mock_client_instance

        result = reversal_search_task(**mock_context)

        assert "Search completed successfully" in result
        assert "Found 0 record(s)" in result

    @patch('reversal_search_tool.reversal_search_tool.read_variable_or_file')
    @patch('reversal_search_tool.reversal_search_tool.bigquery.Client')
    def test_reversal_search_task_name_input(
        self, mock_bq_client, mock_read_config, mock_context, mock_bq_result_row
    ):
        """Test reversal search with name input."""
        mock_read_config.return_value = {'curated_zone_project_id': 'test-project'}
        mock_context['dag_run'].conf = {
            'input_type': 'name',
            'given_name': 'FirstName',
            'surname': 'LastName'
        }

        mock_query_result = MagicMock()
        mock_query_result.total_rows = 1
        mock_query_result.__iter__ = Mock(return_value=iter([mock_bq_result_row]))

        mock_client_instance = MagicMock()
        mock_client_instance.query_and_wait.return_value = mock_query_result
        mock_bq_client.return_value = mock_client_instance

        result = reversal_search_task(**mock_context)

        assert "Search completed successfully" in result
        # Verify query was built with name parameters
        call_args = mock_client_instance.query_and_wait.call_args[0][0]
        assert 'GIVEN_NAME' in call_args
        assert 'SURNAME' in call_args

    @patch('reversal_search_tool.reversal_search_tool.read_variable_or_file')
    @patch('reversal_search_tool.reversal_search_tool.bigquery.Client')
    def test_reversal_search_task_combined_input(
        self, mock_bq_client, mock_read_config, mock_context, mock_bq_result_row
    ):
        """Test reversal search with combined input."""
        mock_read_config.return_value = {'curated_zone_project_id': 'test-project'}
        mock_context['dag_run'].conf = {
            'input_type': 'combined',
            'email': 'test@example.com',
            'given_name': 'FirstName',
            'surname': 'LastName'
        }

        mock_query_result = MagicMock()
        mock_query_result.total_rows = 1
        mock_query_result.__iter__ = Mock(return_value=iter([mock_bq_result_row]))

        mock_client_instance = MagicMock()
        mock_client_instance.query_and_wait.return_value = mock_query_result
        mock_bq_client.return_value = mock_client_instance

        result = reversal_search_task(**mock_context)

        assert "Search completed successfully" in result
        # Verify query includes both email and name conditions
        call_args = mock_client_instance.query_and_wait.call_args[0][0]
        assert 'EMAIL_CONTACT' in call_args
        assert 'GIVEN_NAME' in call_args
        assert 'SURNAME' in call_args

    @patch('reversal_search_tool.reversal_search_tool.read_variable_or_file')
    @patch('reversal_search_tool.reversal_search_tool.bigquery.Client')
    def test_reversal_search_task_multiple_results(
        self, mock_bq_client, mock_read_config, mock_context
    ):
        """Test reversal search with multiple results."""
        mock_read_config.return_value = {'curated_zone_project_id': 'test-project'}
        mock_context['dag_run'].conf = {
            'input_type': 'email',
            'email': 'test@example.com'
        }

        # Create multiple result rows
        rows = []
        for i in range(3):
            row = MagicMock()
            row.get = Mock(side_effect=lambda key, idx=i: {
                'AccountUID': 1234567 + idx,
                'AccountID': f'ACC{123456 + idx}',
                'CustomerUID': 7654321 + idx,
                'CustomerID': f'CUST{123456 + idx}',
                'userID': f'USER{123456 + idx}',
                'ApplicationID': 9876543210 + idx
            }.get(key))
            rows.append(row)

        mock_query_result = MagicMock()
        mock_query_result.total_rows = 3
        mock_query_result.__iter__ = Mock(return_value=iter(rows))

        mock_client_instance = MagicMock()
        mock_client_instance.query_and_wait.return_value = mock_query_result
        mock_bq_client.return_value = mock_client_instance

        result = reversal_search_task(**mock_context)

        assert "Search completed successfully" in result
        assert "Found 3 record(s)" in result
