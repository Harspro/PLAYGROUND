import unittest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
import pandas as pd
from airflow.exceptions import AirflowException
from data_validation.dag_validation.dag_validator import DagValidator
from util.constants import HOURS_DELTA, DAYS_DELTA


class TestDagValidator(unittest.TestCase):

    @patch('data_validation.dag_validation.dag_validator.bigquery.Client')
    @patch('data_validation.dag_validation.dag_validator.check_holiday')
    def test_validate_no_holiday_and_recent_dag_runs(self, mock_check_holiday, mock_bigquery_client):
        mock_check_holiday.return_value = False
        mock_query_result = MagicMock()
        mock_query_df = pd.DataFrame({'MAX_LOAD_TIMESTAMP': [datetime.now() - timedelta(hours=5)]})
        mock_query_result.to_dataframe.return_value = mock_query_df
        mock_bigquery_client.return_value.query.return_value.result.return_value = mock_query_result

        validator = DagValidator(config_filename="dummy.yaml")
        config = {
            'vendor': 'test_vendor',
            'dag_id': 'test_dag',
            HOURS_DELTA: 6
        }

        try:
            validator.validate(config)
        except AirflowException:
            self.fail("validate raised AirflowException unexpectedly!")

    @patch('data_validation.dag_validation.dag_validator.bigquery.Client')
    @patch('data_validation.dag_validation.dag_validator.check_holiday')
    def test_validate_holiday_prevents_run(self, mock_check_holiday, mock_bigquery_client):
        mock_check_holiday.return_value = True
        mock_query_result = MagicMock()
        mock_query_df = pd.DataFrame({'MAX_LOAD_TIMESTAMP': [datetime.now() - timedelta(days=5)]})
        mock_query_result.to_dataframe.return_value = mock_query_df
        mock_bigquery_client.return_value.query.return_value.result.return_value = mock_query_result

        validator = DagValidator(config_filename="dummy.yaml")
        config = {
            'vendor': 'test_vendor',
            'dag_id': 'test_dag',
        }

        # No exception should be raised on holidays
        validator.validate(config)

    @patch('data_validation.dag_validation.dag_validator.bigquery.Client')
    @patch('data_validation.dag_validation.dag_validator.check_holiday')
    def test_validate_dag_not_run_recently_raises_exception(self, mock_check_holiday, mock_bigquery_client):
        mock_check_holiday.return_value = False
        mock_query_result = MagicMock()
        mock_query_df = pd.DataFrame({'MAX_LOAD_TIMESTAMP': [datetime.now() - timedelta(days=5)]})
        mock_query_result.to_dataframe.return_value = mock_query_df
        mock_bigquery_client.return_value.query.return_value.result.return_value = mock_query_result

        validator = DagValidator(config_filename="dummy.yaml")
        config = {
            'vendor': 'test_vendor',
            'dag_id': 'test_dag',
            DAYS_DELTA: 3,
        }

        with self.assertRaises(AirflowException):
            validator.validate(config)
