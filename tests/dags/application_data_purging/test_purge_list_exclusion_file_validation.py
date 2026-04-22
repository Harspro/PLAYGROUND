"""
Unit tests for BigQuery validation utilities.
"""

import pytest
from unittest.mock import patch, MagicMock
from google.cloud import bigquery
from datetime import datetime, timedelta

from dags.application_data_purging.purge_list_exclusion_file_validation import (
    PurgeListExclusionFileValidation
)


@pytest.fixture
def mock_bigquery_client():
    """Fixture to provide mock BigQuery client."""
    client = MagicMock(spec=bigquery.Client)
    return client


@pytest.fixture
def validation_utils():
    """Fixture to provide PurgeListExclusionFileValidation instance."""
    with patch('dags.application_data_purging.purge_list_exclusion_file_validation.bigquery.Client'):
        return PurgeListExclusionFileValidation(project_id='test-project')


@pytest.fixture
def sample_table_schema():
    """Fixture to provide sample table schema."""
    return [
        bigquery.SchemaField("first_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("last_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("dob", "STRING", mode="NULLABLE")
    ]


@pytest.fixture
def sample_validation_result():
    """Fixture to provide sample validation result."""
    return {
        "table_id": "test-project.test_dataset.test_table",
        "overall_valid": True,
        "column_validation": {
            "is_valid": True,
            "missing_columns": [],
            "existing_columns": ["first_name", "last_name", "dob"]
        },
        "field_validation": {
            "is_valid": True,
            "total_violations": 0,
            "null_violations": 0,
            "empty_violations": 0
        },
        "violation_details": [],
        "validation_timestamp": datetime.now().isoformat()
    }


class TestPurgeListExclusionFileValidation:
    """Test class for PurgeListExclusionFileValidation."""

    def test_init(self, validation_utils):
        """Test initialization of PurgeListExclusionFileValidation."""
        assert validation_utils.project_id == 'test-project'

    @patch('dags.application_data_purging.purge_list_exclusion_file_validation.bigquery.Client')
    def test_validate_required_columns_success(self, mock_client_class):
        """Test successful validation of required columns."""
        # Setup
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_table = MagicMock()
        mock_table.schema = [
            bigquery.SchemaField("first_name", "STRING"),
            bigquery.SchemaField("last_name", "STRING"),
            bigquery.SchemaField("dob", "STRING")
        ]
        mock_client.get_table.return_value = mock_table
        utils = PurgeListExclusionFileValidation(project_id='test-project')
        # Test
        result = utils.validate_required_columns(
            table_id='test-project.test_dataset.test_table',
            required_columns=['first_name', 'last_name', 'dob']
        )
        # Assertions
        assert result['is_valid'] is True
        assert result['missing_columns'] == []
        assert result['existing_columns'] == ['first_name', 'last_name', 'dob']

    @patch('dags.application_data_purging.purge_list_exclusion_file_validation.bigquery.Client')
    def test_validate_required_columns_missing(self, mock_client_class):
        """Test validation with missing required columns."""
        # Setup
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_table = MagicMock()
        mock_table.schema = [
            bigquery.SchemaField("first_name", "STRING"),
            bigquery.SchemaField("last_name", "STRING")
        ]
        mock_client.get_table.return_value = mock_table
        utils = PurgeListExclusionFileValidation(project_id='test-project')
        # Test
        result = utils.validate_required_columns(
            table_id='test-project.test_dataset.test_table',
            required_columns=['first_name', 'last_name', 'dob']
        )
        # Assertions
        assert result['is_valid'] is False
        assert result['missing_columns'] == ['dob']
        assert 'error_message' in result

    @patch('dags.application_data_purging.purge_list_exclusion_file_validation.bigquery.Client')
    def test_validate_mandatory_fields_success(self, mock_client_class):
        """Test successful validation of mandatory fields."""
        # Setup
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_query_job = MagicMock()
        mock_result = MagicMock()
        mock_result.total_rows = 100
        mock_result.null_violations = 0
        mock_result.empty_violations = 0
        mock_result.total_violations = 0
        mock_query_job.result.return_value = [mock_result]
        mock_client.query.return_value = mock_query_job
        utils = PurgeListExclusionFileValidation(project_id='test-project')
        # Test
        result = utils.validate_mandatory_fields(
            table_id='test-project.test_dataset.test_table',
            mandatory_fields=['first_name', 'last_name']
        )
        # Assertions
        assert result['is_valid'] is True
        assert result['total_violations'] == 0
        assert result['null_violations'] == 0
        assert result['empty_violations'] == 0

    @patch('dags.application_data_purging.purge_list_exclusion_file_validation.bigquery.Client')
    def test_validate_mandatory_fields_violations(self, mock_client_class):
        """Test validation with mandatory field violations."""
        # Setup
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_query_job = MagicMock()
        mock_result = MagicMock()
        mock_result.total_rows = 100
        mock_result.null_violations = 5
        mock_result.empty_violations = 3
        mock_result.total_violations = 8
        mock_query_job.result.return_value = [mock_result]
        mock_client.query.return_value = mock_query_job
        utils = PurgeListExclusionFileValidation(project_id='test-project')
        # Test
        result = utils.validate_mandatory_fields(
            table_id='test-project.test_dataset.test_table',
            mandatory_fields=['first_name', 'last_name']
        )
        # Assertions
        assert result['is_valid'] is False
        assert result['total_violations'] == 8
        assert result['null_violations'] == 5
        assert result['empty_violations'] == 3
        assert 'error_message' in result

    @patch('dags.application_data_purging.purge_list_exclusion_file_validation.bigquery.Client')
    def test_get_violation_details(self, mock_client_class):
        """Test getting violation details."""
        # Setup
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_query_job = MagicMock()
        # Create mock row objects that behave like BigQuery Row objects

        class MockRow:
            def __init__(self, **kwargs):
                for key, value in kwargs.items():
                    setattr(self, key, value)

            def __iter__(self):
                return iter(self.__dict__.items())
        mock_result1 = MockRow(first_name='John', last_name='', violation_type='EMPTY_VIOLATION')
        mock_result2 = MockRow(first_name='', last_name='Doe', violation_type='NULL_VIOLATION')
        mock_query_job.result.return_value = [mock_result1, mock_result2]
        mock_client.query.return_value = mock_query_job
        utils = PurgeListExclusionFileValidation(project_id='test-project')
        # Test
        result = utils.get_violation_details(
            table_id='test-project.test_dataset.test_table',
            mandatory_fields=['first_name', 'last_name'],
            limit=100
        )
        # Assertions
        assert len(result) == 2
        assert result[0]['first_name'] == 'John'
        assert result[0]['violation_type'] == 'EMPTY_VIOLATION'
        assert result[1]['last_name'] == 'Doe'
        assert result[1]['violation_type'] == 'NULL_VIOLATION'

    @patch('dags.application_data_purging.purge_list_exclusion_file_validation.bigquery.Client')
    def test_validate_dob_format_success(self, mock_client_class):
        """Test successful DOB format validation."""
        # Setup
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_query_job = MagicMock()
        mock_result = MagicMock()
        mock_result.total_rows = 100
        mock_result.invalid_format_count = 0
        mock_result.total_violations = 0
        mock_query_job.result.return_value = [mock_result]
        mock_client.query.return_value = mock_query_job
        utils = PurgeListExclusionFileValidation(project_id='test-project')
        # Test
        result = utils.validate_dob_format(
            table_id='test-project.test_dataset.test_table',
            dob_column='dob',
            expected_format='YYYYMMDD'
        )
        # Assertions
        assert result['is_valid'] is True
        assert result['total_violations'] == 0
        assert result['invalid_format_count'] == 0

    @patch('dags.application_data_purging.purge_list_exclusion_file_validation.bigquery.Client')
    def test_validate_dob_format_failure(self, mock_client_class):
        """Test DOB format validation with violations."""
        # Setup
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_query_job = MagicMock()
        mock_result = MagicMock()
        mock_result.total_rows = 100
        mock_result.invalid_format_count = 5
        mock_result.total_violations = 5
        mock_query_job.result.return_value = [mock_result]
        mock_client.query.return_value = mock_query_job
        utils = PurgeListExclusionFileValidation(project_id='test-project')
        # Test
        result = utils.validate_dob_format(
            table_id='test-project.test_dataset.test_table',
            dob_column='dob',
            expected_format='YYYYMMDD'
        )
        # Assertions
        assert result['is_valid'] is False
        assert result['total_violations'] == 5
        assert result['invalid_format_count'] == 5
        assert 'error_message' in result

    @patch('dags.application_data_purging.purge_list_exclusion_file_validation.bigquery.Client')
    def test_perform_file_validation_success(self, mock_client_class):
        """Test successful comprehensive validation."""
        # Setup
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        # Mock table schema
        mock_table = MagicMock()
        mock_table.schema = [
            bigquery.SchemaField("first_name", "STRING"),
            bigquery.SchemaField("last_name", "STRING"),
            bigquery.SchemaField("dob", "STRING")
        ]
        mock_client.get_table.return_value = mock_table
        # Mock field validation query
        mock_query_job = MagicMock()
        mock_result = MagicMock()
        mock_result.total_rows = 100
        mock_result.null_violations = 0
        mock_result.empty_violations = 0
        mock_result.total_violations = 0
        mock_query_job.result.return_value = [mock_result]
        mock_client.query.return_value = mock_query_job
        utils = PurgeListExclusionFileValidation(project_id='test-project')
        # Test
        result = utils.perform_file_validation(
            table_id='test-project.test_dataset.test_table',
            required_columns=['first_name', 'last_name', 'dob'],
            mandatory_fields=['first_name', 'last_name'],
            dob_column='dob',
            dob_expected_format='YYYYMMDD'
        )
        # Assertions
        assert result['overall_valid'] is True
        assert result['column_validation']['is_valid'] is True
        assert result['field_validation']['is_valid'] is True
        assert result['dob_validation']['is_valid'] is True

    @patch('dags.application_data_purging.purge_list_exclusion_file_validation.bigquery.Client')
    def test_perform_file_validation_failure(self, mock_client_class):
        """Test comprehensive validation with failures."""
        # Setup
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        # Mock table schema with missing column
        mock_table = MagicMock()
        mock_table.schema = [
            bigquery.SchemaField("first_name", "STRING"),
            bigquery.SchemaField("last_name", "STRING")
        ]
        mock_client.get_table.return_value = mock_table
        utils = PurgeListExclusionFileValidation(project_id='test-project')
        # Test
        result = utils.perform_file_validation(
            table_id='test-project.test_dataset.test_table',
            required_columns=['first_name', 'last_name', 'dob'],
            mandatory_fields=['first_name', 'last_name'],
            dob_column='dob',
            dob_expected_format='YYYYMMDD'
        )
        # Assertions
        assert result['overall_valid'] is False
        assert result['column_validation']['is_valid'] is False
        assert 'dob' in result['column_validation']['missing_columns']


class TestIntegration:
    """Integration tests for the validation utilities."""

    @patch('dags.application_data_purging.purge_list_exclusion_file_validation.bigquery.Client')
    def test_end_to_end_validation_workflow(self, mock_client_class):
        """Test complete validation workflow."""
        # Setup
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        # Mock external table creation
        mock_table = MagicMock()
        mock_table.expires = datetime.now() + timedelta(hours=6)
        mock_client.create_table.return_value = mock_table
        # Mock table schema for column validation
        mock_table_schema = MagicMock()
        mock_table_schema.schema = [
            bigquery.SchemaField("first_name", "STRING"),
            bigquery.SchemaField("last_name", "STRING"),
            bigquery.SchemaField("dob", "STRING")
        ]
        mock_client.get_table.return_value = mock_table_schema
        # Mock field validation query
        mock_query_job = MagicMock()
        mock_result = MagicMock()
        mock_result.total_rows = 100
        mock_result.null_violations = 0
        mock_result.empty_violations = 0
        mock_result.total_violations = 0
        mock_query_job.result.return_value = [mock_result]
        mock_client.query.return_value = mock_query_job
        utils = PurgeListExclusionFileValidation(project_id='test-project')
        # Test workflow
        validation_result = utils.perform_file_validation(
            table_id='test-project.test_dataset.test_table',
            required_columns=['first_name', 'last_name', 'dob'],
            mandatory_fields=['first_name', 'last_name'],
            dob_column='dob',
            dob_expected_format='YYYYMMDD'
        )
        # Assertions
        assert validation_result['overall_valid'] is True
        assert validation_result['column_validation']['is_valid'] is True
        assert validation_result['field_validation']['is_valid'] is True
        assert validation_result['dob_validation']['is_valid'] is True
