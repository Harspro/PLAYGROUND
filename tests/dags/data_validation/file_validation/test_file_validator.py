import unittest
from unittest.mock import patch
from datetime import datetime
from airflow.exceptions import AirflowException
from data_validation.file_validation.file_validator import FileValidator
from util.constants import SOURCE_BUCKET, FOLDER_NAME, FILE_PREFIX, HOURS_DELTA, DAYS_DELTA, HOLIDAY_CHECK_DELTA


class TestFileValidator(unittest.TestCase):

    @patch('data_validation.file_validation.file_validator.list_blobs_with_prefix')
    @patch('data_validation.file_validation.file_validator.current_datetime_toronto')
    @patch('data_validation.file_validation.file_validator.read_variable_or_file')
    def test_validate_file_exists(self, mock_read_variable_or_file, mock_current_datetime, mock_list_blobs):
        mock_read_variable_or_file.return_value = {'DEPLOY_ENV_STORAGE_SUFFIX': '-dev'}
        mock_current_datetime.return_value = datetime(2024, 1, 1)
        mock_list_blobs.return_value = ['file1']

        validator = FileValidator(config_filename="mock_file.yaml")
        config = {
            'vendor': 'test_vendor',
            SOURCE_BUCKET: 'test_bucket',
            FOLDER_NAME: 'test_folder',
            FILE_PREFIX: 'test_prefix'
        }

        try:
            validator.validate(config)
        except AirflowException:
            self.fail("validate raised AirflowException unexpectedly!")

    @patch('data_validation.file_validation.file_validator.list_blobs_with_prefix')
    @patch('data_validation.file_validation.file_validator.current_datetime_toronto')
    @patch('data_validation.file_validation.file_validator.read_variable_or_file')
    def test_validate_file_with_hours_delta(self, mock_read_variable_or_file, mock_current_datetime, mock_list_blobs):
        mock_read_variable_or_file.return_value = {'DEPLOY_ENV_STORAGE_SUFFIX': '-dev'}
        mock_current_datetime.return_value = datetime(2024, 1, 1, 12)
        mock_list_blobs.side_effect = [[], ['file1']]

        validator = FileValidator(config_filename="mock_file.yaml")
        config = {
            'vendor': 'test_vendor',
            SOURCE_BUCKET: 'test_bucket',
            FOLDER_NAME: 'test_folder',
            FILE_PREFIX: 'test_prefix',
            HOURS_DELTA: 2,  # Check the past 2 hours
        }

        try:
            validator.validate(config)
        except AirflowException:
            self.fail("validate raised AirflowException unexpectedly!")

    @patch('data_validation.file_validation.file_validator.list_blobs_with_prefix')
    @patch('data_validation.file_validation.file_validator.current_datetime_toronto')
    @patch('data_validation.file_validation.file_validator.read_variable_or_file')
    def test_validate_file_with_days_delta(self, mock_read_variable_or_file, mock_current_datetime, mock_list_blobs):
        mock_read_variable_or_file.return_value = {'DEPLOY_ENV_STORAGE_SUFFIX': '-dev'}
        mock_current_datetime.return_value = datetime(2024, 1, 3)
        mock_list_blobs.side_effect = [[], [], ['file1']]

        validator = FileValidator(config_filename="mock_file.yaml")
        config = {
            'vendor': 'test_vendor',
            SOURCE_BUCKET: 'test_bucket',
            FOLDER_NAME: 'test_folder',
            FILE_PREFIX: 'test_prefix',
            DAYS_DELTA: 2,  # Check the past 2 days
        }

        try:
            validator.validate(config)
        except AirflowException:
            self.fail("validate raised AirflowException unexpectedly!")

    @patch('data_validation.file_validation.file_validator.list_blobs_with_prefix')
    @patch('data_validation.file_validation.file_validator.current_datetime_toronto')
    @patch('data_validation.file_validation.file_validator.read_variable_or_file')
    def test_validate_file_does_not_exist_and_not_holiday(self, mock_read_variable_or_file, mock_current_datetime, mock_list_blobs):
        mock_read_variable_or_file.return_value = {'DEPLOY_ENV_STORAGE_SUFFIX': '-dev'}
        mock_current_datetime.return_value = datetime(2024, 1, 1)
        mock_list_blobs.return_value = []  # No files

        validator = FileValidator(config_filename="mock_file.yaml")
        config = {
            'vendor': 'test_vendor',
            SOURCE_BUCKET: 'test_bucket',
            FOLDER_NAME: 'test_folder',
            FILE_PREFIX: 'test_prefix',
            HOLIDAY_CHECK_DELTA: 0
        }

        with patch('data_validation.file_validation.file_validator.check_holiday', return_value=False):
            with self.assertRaises(AirflowException):
                validator.validate(config)

    @patch('data_validation.file_validation.file_validator.list_blobs_with_prefix')
    @patch('data_validation.file_validation.file_validator.current_datetime_toronto')
    @patch('data_validation.file_validation.file_validator.read_variable_or_file')
    def test_validate_file_does_not_exist_and_holiday(self, mock_read_variable_or_file, mock_current_datetime, mock_list_blobs):
        mock_read_variable_or_file.return_value = {'DEPLOY_ENV_STORAGE_SUFFIX': '-dev'}
        mock_current_datetime.return_value = datetime(2024, 1, 1)
        mock_list_blobs.return_value = []  # No files

        validator = FileValidator(config_filename="mock_file.yaml")
        config = {
            'vendor': 'test_vendor',
            SOURCE_BUCKET: 'test_bucket',
            FOLDER_NAME: 'test_folder',
            FILE_PREFIX: 'test_prefix',
            HOLIDAY_CHECK_DELTA: 0
        }

        with patch('data_validation.file_validation.file_validator.check_holiday', return_value=True):
            try:
                validator.validate(config)
            except AirflowException:
                self.fail("validate raised AirflowException unexpectedly!")
