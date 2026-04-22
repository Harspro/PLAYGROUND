"""
Tests for the custom `PcfBigQueryToPostgresOperator` class.

Author: Sharif Mansour
"""

from __future__ import annotations
from datetime import datetime
from unittest.mock import patch, MagicMock, call, PropertyMock

import pytest

from pcf_operators.bigquery_operators.bigquery_to_postgres import PcfBigQueryToPostgresOperator
from psycopg2 import errors
from airflow.exceptions import AirflowFailException


@pytest.fixture
def test_bigquery_to_postgres_data() -> dict:
    """
    Pytest fixture providing test data for Airflow PCF Bigquery to Postgres testing.

    It returns a dictionary that includes sample values for task id, postgres
    connection id, target table name, sample selected fields, bigquery batch size,
    sql batch size, dataset id, table id, and sample records.

    :returns: A dictionary containing test data for when connecting pcf BQ to Postgres.
    :rtype: dict
    """
    return {
        'task_id': 'test-task-id',
        'postgres_conn_id': 'test-postgres-connection-id',
        'target_table_name': 'test-destination-table',
        'selected_fields': ['id', 'name'],
        'bigquery_batch_size': 100000,
        'sql_batch_size': 10000,
        'dataset_id': 'test-dataset-id',
        'table_id': 'test-table-id'
    }


def make_unique_violation():
    exc = errors.UniqueViolation("duplicate key value violates unique constraint")
    type(exc).pgcode = PropertyMock(return_value="23505")
    type(exc).pgerror = PropertyMock(return_value=(
        "ERROR: duplicate key value violates unique constraint \"test_pkey\"\n"
        "Key (card_number, trace_id, hdr_id, update_year, update_month)=(5181271035032405, "
        "02190313543, 4832059.000000000, 2010, 9) already exists.\n"
    ))
    return exc


class TestPcfBigQueryToPostgresOperator:
    """
    This test suite covers the functionality of the `PcfBigQueryToPostgresOperator` class.

    The operator is responsible for transferring data from BigQuery tables to PostgreSQL tables.

    The tests validate various aspects of the operator's functionality, including:
    - Data transfer from BigQuery to PostgreSQL.
    - Handling of different data types and formats.
    - Error handling for invalid configurations or data.

    Usage:
    - Run the tests using the pytest command.
    - Utilize coverage package to determine test coverage on operator.
    """

    @pytest.mark.parametrize("selected_fields, database_type, replace, update_fields, conflict_fields, append_fields",
                             [
                                 (['id', 'name', 'city', 'amount'], "Postgres", False, ['name', 'city', 'amount'], ['id'], ['amount']),
                                 (['id', 'name', 'city'], "Postgres", False, ['name', 'city'], ['id'], None),
                                 (['id', 'name'], "Postgres", False, ['name'], ['id'], None),
                                 (['id', 'name'], "Postgres", False, None, ['id'], None),
                                 (['id', 'name'], "Postgres", False, None, None, None),
                                 (['id', 'name'], "Postgres", True, None, None, None),
                                 (None, "Postgres", False, ['name', 'city'], ['id'], None),
                                 (None, "Postgres", False, ['name'], ['id'], None),
                                 (None, "Postgres", False, None, ['id'], None),
                                 (None, "Postgres", False, None, None, None),
                                 (None, "Postgres", True, None, ['id'], None)
                             ])
    @patch("pcf_operators.bigquery_operators.bigquery_to_sql.execute_batch")
    @patch("pcf_operators.bigquery_operators.bigquery_to_sql.bigquery_get_data")
    @patch("pcf_operators.bigquery_operators.bigquery_to_postgres.PostgresHook")
    @patch("pcf_operators.bigquery_operators.bigquery_to_sql.BigQueryHook")
    def test_execute_good_request_to_bq(self, mock_bigquery_hook, mock_postgres_hook, mock_bigquery_get_data,
                                        mock_execute_batch, selected_fields: list[str] | None,
                                        database_type: str | None, replace: bool, update_fields: list[str] | None,
                                        conflict_fields: list[str] | None, append_fields: list[str] | None,
                                        test_bigquery_to_postgres_data: dict):
        """
        Test happy path for the `PcfBigQueryToPostgresOperator` class. This is used to
        simulate extracting BigQuery data and writing it to a SQL Database.

        This test function is parameterized to run with different combinations of
        `selected_fields`, `database_type`, whether to `replace` the target table data,
         `update_fields` if records exist in the table, and `conflict_fields` which are
         the primary keys on the table.

        :param mock_bigquery_hook: Fixture providing a mock object for the hook used to
            interact with BigQuery.
        :type mock_bigquery_hook: :class:`unittest.mock.MagicMock`

        :param mock_postgres_hook: Fixture providing a mock object for the hook used to
            interact with Postgres.
        :type mock_postgres_hook: :class:`unittest.mock.MagicMock`

        :param mock_bigquery_get_data: Fixture providing a mock object for retrieving
            BigQuery Data.
        :type mock_bigquery_get_data: :class:`unittest.mock.MagicMock`

        :param mock_execute_batch: Fixture providing a mock object for executing
            batches on SQL server.
        :type mock_execute_batch: :class:`unittest.mock.MagicMock`

        :param selected_fields: List of fields to return (comma-separated). If unspecified,
            all fields are returned.
        :type selected_fields: list[str] or None

        :param database_type: Type of database to use.
        :type database_type: str or None

        :param replace: Whether to replace instead of insert.
        :type replace: bool

        :param update_fields: List of fields to update during insert conflict.
        :type update_fields: list[str] or None

        :param conflict_fields: List of primary key fields.
        :type conflict_fields: list[str] or None

        :param append_fields: List of append fields.
        :type append_fields: list[str] or None

        :param test_bigquery_to_postgres_data: Dictionary of fixture test data for BigQuery to Postgres test cases.
        :type test_bigquery_to_postgres_data: dict
        """

        # Create an instance of the `PcfBigQueryToPostgresOperator`.
        operator = PcfBigQueryToPostgresOperator(
            task_id=test_bigquery_to_postgres_data['task_id'],
            dataset_table=f"{test_bigquery_to_postgres_data['dataset_id']}."
                          f"{test_bigquery_to_postgres_data['table_id']}",
            postgres_conn_id=test_bigquery_to_postgres_data['postgres_conn_id'],
            target_table_name=test_bigquery_to_postgres_data['target_table_name'],
            selected_fields=selected_fields,
            database_type=database_type,
            replace=replace,
            bigquery_batch_size=test_bigquery_to_postgres_data['bigquery_batch_size'],
            sql_batch_size=test_bigquery_to_postgres_data['sql_batch_size'],
            update_fields=update_fields,
            conflict_fields=conflict_fields,
            append_fields=append_fields,
            execution_method="batch"
        )

        # Mocking a cursor.
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connection.encoding = 'UTF8'
        mock_cursor.connection = mock_connection
        mock_postgres_hook.return_value.get_conn.return_value.cursor.return_value = mock_cursor

        # Mocking return value based on test case.
        if update_fields and len(update_fields) > 1 and append_fields:
            mock_bigquery_get_data.return_value = [
                [(1, 'Alice', 'Toronto', 10)],
                [(datetime(2022, 4, 19, 12, 30, 0), None, None, None)]
            ]
            expected_rows = [[('1', 'Alice', 'Toronto', '10')], [('2022-04-19T12:30:00', None, None, None)]]
        elif update_fields and len(update_fields) > 1:
            mock_bigquery_get_data.return_value = [
                [(1, 'Alice', 'Toronto')],
                [(datetime(2022, 4, 19, 12, 30, 0), None, None)]
            ]
            expected_rows = [[('1', 'Alice', 'Toronto')], [('2022-04-19T12:30:00', None, None)]]
        else:
            mock_bigquery_get_data.return_value = [
                [(1, 'Alice')],
                [(datetime(2022, 4, 19, 12, 30, 0), None)]
            ]
            expected_rows = [[('1', 'Alice')], [('2022-04-19T12:30:00', None)]]

        # Execute the operator.
        operator.execute(context=MagicMock())

        # Verifying the BigQuery hook is called once.
        mock_bigquery_hook.assert_called_once()

        # Verifying the Postgres hook is called once with the correct arguments.
        mock_postgres_hook.assert_called_once_with(
            schema=None,
            postgres_conn_id=test_bigquery_to_postgres_data['postgres_conn_id']
        )

        # Verifying the BigQuery Get Data function is called once with the correct arguments.
        mock_bigquery_get_data.assert_called_once_with(
            logger=operator.log,
            dataset_id=test_bigquery_to_postgres_data['dataset_id'],
            table_id=test_bigquery_to_postgres_data['table_id'],
            big_query_hook=mock_bigquery_hook(),
            batch_size=test_bigquery_to_postgres_data['bigquery_batch_size'],
            selected_fields=selected_fields
        )

        expected_calls = []

        # Creating expected calls based on the parametrized tests for Postgres DB.
        if database_type.lower() == "postgres":
            if replace:
                if selected_fields is not None:
                    # Expected sql statement for replace using selected fields.
                    expected_sql_statement = 'REPLACE INTO test-destination-table ("id", "name") VALUES (%s, %s);'
                else:
                    # Expected sql statement for replace without selected fields.
                    expected_sql_statement = 'REPLACE INTO test-destination-table  VALUES (%s, %s);'
            else:
                if selected_fields is not None:
                    if update_fields and conflict_fields and append_fields:
                        if len(update_fields) > 1:
                            # Expected sql statement for insert with selected, update, conflict, and append fields.
                            expected_sql_statement = ('INSERT INTO test-destination-table '
                                                      '("id", "name", "city", "amount") '
                                                      'VALUES (%s, %s, %s, %s) ON CONFLICT ("id") DO UPDATE '
                                                      'SET ("name", "city", "amount") = '
                                                      '(EXCLUDED."name", EXCLUDED."city", '
                                                      'EXCLUDED."amount"+test-destination-table."amount");')
                    elif update_fields and conflict_fields:
                        if len(update_fields) > 1:
                            # Expected sql statement for insert with selected, 1 update, and conflict fields.
                            expected_sql_statement = ('INSERT INTO test-destination-table ("id", "name", "city") '
                                                      'VALUES (%s, %s, %s) ON CONFLICT ("id") DO UPDATE '
                                                      'SET ("name", "city") = (EXCLUDED."name", EXCLUDED."city");')
                        else:
                            # Expected sql statement for insert with selected, 1 update, and conflict fields.
                            expected_sql_statement = ('INSERT INTO test-destination-table ("id", "name") '
                                                      'VALUES (%s, %s) ON CONFLICT ("id") DO UPDATE '
                                                      'SET "name" = EXCLUDED."name";')
                    elif conflict_fields:
                        # Expected sql statement for insert with selected and conflict fields.
                        expected_sql_statement = ('INSERT INTO test-destination-table ("id", "name") VALUES (%s, %s) '
                                                  'ON CONFLICT ("id") DO NOTHING;')
                    else:
                        # Expected sql statement for insert with selected fields.
                        expected_sql_statement = 'INSERT INTO test-destination-table ("id", "name") VALUES (%s, %s);'
                else:
                    if update_fields and conflict_fields:
                        if len(update_fields) > 1:
                            # Expected sql statement for insert with 1 update and conflict fields
                            # but without selected fields.
                            expected_sql_statement = ('INSERT INTO test-destination-table  VALUES (%s, %s, %s) '
                                                      'ON CONFLICT ("id") DO UPDATE SET ("name", "city") = '
                                                      '(EXCLUDED."name", EXCLUDED."city");')
                        else:
                            # Expected sql statement for insert with 1 update and conflict fields
                            # but without selected fields.
                            expected_sql_statement = ('INSERT INTO test-destination-table  VALUES (%s, %s) '
                                                      'ON CONFLICT ("id") DO UPDATE SET "name" = '
                                                      'EXCLUDED."name";')
                    elif conflict_fields:
                        # Expected sql statement for insert with conflict fields but without selected and update fields.
                        expected_sql_statement = ('INSERT INTO test-destination-table  VALUES (%s, %s) '
                                                  'ON CONFLICT ("id") DO NOTHING;')
                    else:
                        # Expected sql statement for insert without selected, conflict, and update fields.
                        expected_sql_statement = 'INSERT INTO test-destination-table  VALUES (%s, %s);'

            for row in expected_rows:
                expected_calls.append(
                    call(
                        mock_cursor,
                        expected_sql_statement,
                        row,
                        page_size=test_bigquery_to_postgres_data['sql_batch_size']
                    )
                )

            # Verifying the execute batch function is utilized with the correct calls.
            mock_execute_batch.assert_has_calls(expected_calls)

    def test_dataset_table_parsing_error(self, test_bigquery_to_postgres_data: dict):
        """
        Test case validating error handling mechanism in the `PcfBigQueryToPostgresOperator`
        class when parsing the `dataset_table` parameter. It verifies that the operator
        raises a `ValueError` when the `dataset_table` parameter is invalid, and checks
        if the correct error message is raised.

        :param test_bigquery_to_postgres_data: Fixture providing a dictionary containing
            test data when utilizing the PcfBigQueryToPostgresOperator.
        :type test_bigquery_to_postgres_data: dict
        """

        # Test dataset_table parsing error
        with pytest.raises(ValueError) as e:
            PcfBigQueryToPostgresOperator(
                task_id=test_bigquery_to_postgres_data['task_id'],
                dataset_table="invalid_dataset_table",
                postgres_conn_id=test_bigquery_to_postgres_data['postgres_conn_id'],
                target_table_name=test_bigquery_to_postgres_data['target_table_name'],
                selected_fields=test_bigquery_to_postgres_data['selected_fields']
            )

        # Check if the correct error message is raised
        assert str(e.value) == "Could not parse invalid_dataset_table as <dataset>.<table>"

    @patch("pcf_operators.bigquery_operators.bigquery_to_sql.execute_values")
    @patch("pcf_operators.bigquery_operators.bigquery_to_sql.bigquery_get_data")
    @patch("pcf_operators.bigquery_operators.bigquery_to_postgres.PostgresHook")
    @patch("pcf_operators.bigquery_operators.bigquery_to_sql.BigQueryHook")
    def test_execute_failure(self, mock_bigquery_hook, mock_postgres_hook, mock_bigquery_get_data, mock_execute_values,
                             test_bigquery_to_postgres_data: dict):
        """
        Test happy path for the `PcfBigQueryToPostgresOperator` class. This is used to
        simulate extracting BigQuery data and writing it to a SQL Database.

        This test function is parameterized to run with different combinations of
        `selected_fields`, `database_type`, whether to `replace` the target table data,
         `update_fields` if records exist in the table, and `conflict_fields` which are
         the primary keys on the table.

        :param mock_bigquery_hook: Fixture providing a mock object for the hook used to
            interact with BigQuery.
        :type mock_bigquery_hook: :class:`unittest.mock.MagicMock`

        :param mock_postgres_hook: Fixture providing a mock object for the hook used to
            interact with Postgres.
        :type mock_postgres_hook: :class:`unittest.mock.MagicMock`

        :param mock_bigquery_get_data: Fixture providing a mock object for retrieving
            BigQuery Data.
        :type mock_bigquery_get_data: :class:`unittest.mock.MagicMock`

        :param mock_execute_values: Fixture providing a mock object for executing
             on SQL server.
        :type mock_execute_values: :class:`unittest.mock.MagicMock`

        """

        # Create an instance of the `PcfBigQueryToPostgresOperator`.
        operator = PcfBigQueryToPostgresOperator(
            task_id=test_bigquery_to_postgres_data['task_id'],
            dataset_table=f"{test_bigquery_to_postgres_data['dataset_id']}."
                          f"{test_bigquery_to_postgres_data['table_id']}",
            postgres_conn_id=test_bigquery_to_postgres_data['postgres_conn_id'],
            target_table_name=test_bigquery_to_postgres_data['target_table_name'],
            selected_fields=['id', 'name', 'city', 'amount'],
            database_type="Postgres",
            replace=None,
            bigquery_batch_size=test_bigquery_to_postgres_data['bigquery_batch_size'],
            sql_batch_size=test_bigquery_to_postgres_data['sql_batch_size'],
            update_fields=None,
            conflict_fields=None,
            append_fields=None,
            execution_method="values"
        )

        # Mocking a cursor.
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connection.encoding = 'UTF8'
        mock_cursor.connection = mock_connection
        mock_postgres_hook.return_value.get_conn.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_execute_values.side_effect = make_unique_violation()
        # Mocking return value based on test case.
        mock_bigquery_get_data.return_value = [
            [(1, 'Alice')],
            [(datetime(2022, 4, 19, 12, 30, 0), None)]
        ]

        # Execute the operator.
        with pytest.raises(AirflowFailException) as exc_info:
            operator.execute(context=MagicMock())
        mock_connection.rollback.assert_called_once()
        assert "Database error occurred. Type: UniqueViolation" in str(exc_info.value)
