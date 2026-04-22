import unittest
import pandas as pd
from unittest.mock import patch, MagicMock

from airflow.operators.python import PythonOperator
from re_idv_request_processing.re_idv_request_processing import ReIdvRequestKafkaProcessor

DAG_ID = "re_idv_request_kafka_processing"
CONFIG = "re_idv_request_processing_config.yaml"


class TestReIdvRequestKafkaProcessor(unittest.TestCase):

    @patch("re_idv_request_processing.re_idv_request_processing.ReIdvRequestKafkaProcessor.get_re_idv_customer_sql")
    def test_export_task_exists_and_configured(self, mock_get_sql):

        mock_get_sql.return_value = "SELECT * FROM fake_table"

        processor = ReIdvRequestKafkaProcessor(CONFIG)
        dag_dict = processor.create()
        dag = dag_dict[DAG_ID]

        task = dag.get_task("export_data_to_parquet_from_bigquery")

        self.assertIsNotNone(task)
        self.assertIsInstance(task, PythonOperator)
        self.assertEqual(task.python_callable.__name__, processor.export_data_to_parquet_from_bigquery_fn.__name__)

    @patch("re_idv_request_processing.re_idv_request_processing.run_bq_query")
    @patch("re_idv_request_processing.re_idv_request_processing.ReIdvRequestKafkaProcessor.get_re_idv_customer_sql")
    def test_customer_count_greater_than_zero(self, mock_get_sql, mock_run_bq_query):
        mock_get_sql.return_value = "SELECT COUNT(*) AS customer_count FROM table"

        df = pd.DataFrame({'customer_count': [5]})
        mock_result = MagicMock()
        mock_result.to_dataframe.return_value = df
        mock_run_bq_query.return_value = mock_result

        processor = ReIdvRequestKafkaProcessor(CONFIG)

        result = processor.evaluate_customer_count()

        self.assertEqual(result, 'export_data_to_parquet_from_bigquery')

    @patch("re_idv_request_processing.re_idv_request_processing.run_bq_query")
    @patch("re_idv_request_processing.re_idv_request_processing.ReIdvRequestKafkaProcessor.get_re_idv_customer_sql")
    def test_customer_count_zero(self, mock_get_sql, mock_run_bq_query):
        mock_get_sql.return_value = "SELECT COUNT(*) AS customer_count FROM table"

        df = pd.DataFrame({'customer_count': [0]})
        mock_result = MagicMock()
        mock_result.to_dataframe.return_value = df
        mock_run_bq_query.return_value = mock_result

        processor = ReIdvRequestKafkaProcessor(CONFIG)

        result = processor.evaluate_customer_count()

        self.assertEqual(result, 'no_records_task')

    @patch("re_idv_request_processing.re_idv_request_processing.read_file_env")
    def test_sql_placeholders_are_replaced(self, mock_read_file_env):
        raw_sql = """
        SELECT
        CUSTOMER_UID,
        UUID,
        EMAIL_SENT_COUNTER
        FROM latest_records
        WHERE rn = 1
          AND (
            UPPER(RE_IDV_STATUS) = 'NEW'
            OR (
                UPPER(RE_IDV_STATUS) = 'PENDING'
                AND EMAIL_SENT_COUNTER < {max_reminder_count}
                AND (
                    STATUS_CHANGE_TIMESTAMP IS NULL
                    OR STATUS_CHANGE_TIMESTAMP <= CURRENT_TIMESTAMP() - INTERVAL {min_hours_between_reminders} HOUR
                )
            )
          )
        """
        mock_read_file_env.return_value = raw_sql

        processor = ReIdvRequestKafkaProcessor(CONFIG)

        processor.job_config["dag"]["reminder_config"] = {
            "max_reminder_count": 3,
            "min_hours_between_reminders": 48
        }
        mock_context = {
            "dag_run": MagicMock(conf={"min_hours_between_reminders": None})
        }

        formatted_sql = processor.get_re_idv_customer_sql("fake.sql", **mock_context)

        self.assertIn("EMAIL_SENT_COUNTER < 3", formatted_sql)
        self.assertIn("STATUS_CHANGE_TIMESTAMP <= CURRENT_TIMESTAMP() - INTERVAL 48 HOUR", formatted_sql)
