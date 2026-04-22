"""
PCF Base operator for BigQuery to SQL operators.

Author: Sharif Mansour
"""
from __future__ import annotations

import abc

from contextlib import closing
from datetime import datetime
from typing import TYPE_CHECKING, Sequence

from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.utils.bigquery_get_data import bigquery_get_data
from airflow.exceptions import AirflowFailException

from more_itertools import chunked
from psycopg2.extras import execute_batch, execute_values

if TYPE_CHECKING:
    from airflow.providers.common.sql.hooks.sql import DbApiHook
    from airflow.utils.context import Context


class PcfBigQueryToSqlBaseOperator(BaseOperator):
    """
    This is an extension of the BigQueryToSqlBaseOperator, the current operator performs poorly when
    executing insert statements. Approximately a rate of 100 records per second, below improvements
    increase the execution rate to approximately 10,000 records per second based on early testing.

    The operator is responsible for fetching data from a BigQuery table (alternatively fetch
    selected columns) and inserting it into an SQL table.

    Performance tuning is built to primarily support writing to Postgres for now.

    .. note::
        If you pass fields to ``selected_fields`` which are in different order than the
        order of columns already in
        BQ table, the data will still be in the order of BQ table.
        For example if the BQ table has 3 columns as
        ``[A,B,C]`` and you pass 'B,A' in the ``selected_fields``
        the data would still be of the form ``'A,B'`` and passed through this form
        to the SQL database.

    :param dataset_table: A dotted ``<dataset>.<table>``: the big query table of origin.
    :type dataset_table: str

    :param target_table_name: Target SQL table.
    :type target_table_name: str or None

    :param selected_fields: List of fields to return (comma-separated). If unspecified, all fields
        are returned.
    :type selected_fields: list[str] or str or None, default None

    :param gcp_conn_id: Reference to a specific Google Cloud hook.
    :type gcp_conn_id: str, default "google_cloud_default"

    :param database_type: Type of database to use.
    :type database_type: str or None, default None

    :param database: name of database which overwrite defined one in connection
    :type database: str or None, default None

    :param replace: Whether to replace instead of insert.
    :type replace: bool, default False

    :param update_fields: Fields to update if records exist in table.
    :type update_fields: list[str] or None, default None

    :param conflict_fields: Conflict fields in table.
    :type conflict_fields: list[str] or None, default None

    :param append_fields: Fields to use when appending new values.
    :type append_fields: list[str] or None, default None

    :param bigquery_batch_size: The size of the batch to use when reading from BigQuery.
    :type bigquery_batch_size: int, default 1000

    :param sql_batch_size: The size of the batch to use when writing to the sql Database.
    :type sql_batch_size: int, default 1000

    :param location: The location used for the operation.
    :type location: str or None, default None

    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: str or Sequence[str] or None, default None

    :param dataset_id: BigQuery dataset id to use.
    :type dataset_id: str or None, default None

    :param table_id: BigQuery table id to use.
    :type table_id: str or None, default None

    :param execution_method: Query execution method to use.
    :type execution_method: str or None, default None
    """

    template_fields: Sequence[str] = (
        "target_table_name",
        "impersonation_chain",
        "dataset_id",
        "table_id",
    )

    def __init__(
            self,
            *,
            dataset_table: str | None = None,
            target_table_name: str | None = None,
            selected_fields: list[str] | str | None = None,
            gcp_conn_id: str = "google_cloud_default",
            database_type: str | None = None,
            database: str | None = None,
            replace: bool = False,
            update_fields: list[str] | None = None,
            conflict_fields: list[str] | None = None,
            append_fields: list[str] | None = None,
            bigquery_batch_size: int = 1000,
            sql_batch_size: int = 1000,
            location: str | None = None,
            impersonation_chain: str | Sequence[str] | None = None,
            dataset_id: str | None = None,
            table_id: str | None = None,
            execution_method: str | None = None,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.target_table_name = target_table_name
        self.selected_fields = selected_fields
        self.gcp_conn_id = gcp_conn_id
        self.database_type = database_type
        self.database = database
        self.replace = replace
        self.update_fields = update_fields
        self.conflict_fields = conflict_fields
        self.append_fields = append_fields
        self.bigquery_batch_size = bigquery_batch_size
        self.sql_batch_size = sql_batch_size
        self.location = location
        self.impersonation_chain = impersonation_chain
        self.dataset_id = dataset_id
        self.table_id = table_id
        # Validate and store the execution method
        if not execution_method:
            execution_method = "auto"
        valid_methods = ["auto", "values", "batch"]
        execution_method = execution_method.lower()
        if execution_method not in valid_methods:
            raise ValueError(
                f"Invalid execution_method '{execution_method}'. "
                f"Must be one of: {', '.join(valid_methods)}"
            )
        self.execution_method = execution_method

        try:
            if dataset_table:
                self.dataset_id, self.table_id = dataset_table.split(".")
        except ValueError:
            raise ValueError(f"Could not parse {dataset_table} as <dataset>.<table>") from None

    @abc.abstractmethod
    def get_sql_hook(self) -> DbApiHook:
        """Return a concrete SQL Hook (a PostgresHook for instance)."""

    @abc.abstractmethod
    def get_db_error(self, ex: Exception) -> dict:
        """Return the db specific error. all db impl can have their own impl to remove
         detail error which might have PII data"""

    def persist_links(self, context: Context) -> None:
        """Persist the connection to the SQL provider."""

    def _determine_execution_method(self) -> str:
        """
            Determines which execution method to use based on configuration and operation type.

            Returns:
            str: Either "values" or "batch" depending on the configuration and operation type.
        """
        if self.execution_method != "auto":
            # If explicitly configured, use the specified method
            return self.execution_method

        # In auto mode, use execute_batch only for updates
        if self.update_fields:
            return "batch"
        return "values"

    @staticmethod
    def _generate_batch_sql(
            table: str, values: tuple[str | None], target_fields: str, replace: bool,
            update_fields: list[str], conflict_fields: list[str], append_fields: list[str]
    ) -> str:
        """
        Generate the INSERT SQL statement to be executed on the Database.

        :param table: Name of the target table.
        :type table: str

        :param values: The row to insert into the table.
        :type values: str

        :param target_fields: The names of the columns to fill in the table.
        :type target_fields: str

        :param replace: Whether to replace/upsert instead of insert.
        :type replace: bool

        :param update_fields: Fields to update if records exist in table.
        :type update_fields: list[str]

        :param conflict_fields: Conflict fields in table.
        :type conflict_fields: list[str]

        :param append_fields: Instead of updating these values, append the new value
            to the current one in the DB.
        :type append_fields: list[str]

        :return: The generated INSERT or REPLACE/UPSERT SQL statement.
        :rtype: str

        """
        placeholders = ["%s", ] * len(values)
        target_fields_str = "(" + ', '.join(f'"{tf}"' for tf in target_fields) + ")" if target_fields else ""
        conflict_fields_str = "(" + ', '.join(f'"{cf}"' for cf in conflict_fields) + ")" if conflict_fields else ""

        if replace:
            return f"REPLACE INTO {table} {target_fields_str} VALUES ({', '.join(placeholders)});"
        else:
            if conflict_fields_str and update_fields:
                if append_fields:
                    excluded_fields_str = [
                        f'EXCLUDED."{field}"+{table}."{field}"' if field in append_fields else
                        f'EXCLUDED."{field}"' for field in update_fields
                    ]
                else:
                    excluded_fields_str = [f'EXCLUDED."{field}"' for field in update_fields]

                if len(excluded_fields_str) > 1:
                    update_fields_str = ("(" + ', '.join(f'"{uf}"' for uf in update_fields) + ")"
                                         + f" = ({', '.join(excluded_fields_str)})")
                else:
                    update_fields_str = f'"{update_fields[0]}" = {excluded_fields_str[0]}'

                return (f"INSERT INTO {table} {target_fields_str} VALUES ({', '.join(placeholders)}) "
                        f"ON CONFLICT {conflict_fields_str} DO UPDATE SET {update_fields_str};")
            elif conflict_fields_str:
                return (f"INSERT INTO {table} {target_fields_str} VALUES ({', '.join(placeholders)}) "
                        f"ON CONFLICT {conflict_fields_str} DO NOTHING;")
            else:
                return f"INSERT INTO {table} {target_fields_str} VALUES ({', '.join(placeholders)});"

    @staticmethod
    def _generate_values_sql(
        table: str,
        target_fields: str,
        replace: bool = False,
        update_fields: list[str] | None = None,
        conflict_fields: list[str] | None = None,
        append_fields: list[str] | None = None
    ) -> tuple[str, str]:
        """
            Generate SQL for execute_values with a single %s placeholder.
            This method handles all types of inserts including:
            1. Simple inserts
            2. Replace operations
            3. Insert with ON CONFLICT DO NOTHING
            4. Insert with ON CONFLICT DO UPDATE
            5. Insert with ON CONFLICT DO UPDATE with value appending

            The key difference from _generate_batch_sql is that this method creates SQL
            compatible with execute_values, which requires a single %s placeholder that
            will be replaced with multiple (column1, column2, ...) values.

            Args:
                table: Name of the target table
                target_fields: List of column names
                replace: Whether to perform a REPLACE operation
                update_fields: Fields to update on conflict
                conflict_fields: Fields that determine a conflict
                append_fields: Fields whose values should be appended on conflict

            Returns:
                tuple[str, str]: A tuple containing:
                - The SQL statement with a single %s placeholder
                - The template string for formatting each row of values
        """
        # Create the target fields string for the column names
        target_fields_str = "(" + ', '.join(f'"{tf}"' for tf in target_fields) + ")" if target_fields else ""

        # Create the template for each row of values
        # This is what execute_values will use to format each row
        template = "(" + ", ".join(["%s"] * len(target_fields)) + ")"

        # Start building the SQL statement
        if replace:
            # Handle REPLACE operations
            sql = f"REPLACE INTO {table} {target_fields_str} VALUES %s"
        else:
            # Start with base INSERT statement
            sql = f"INSERT INTO {table} {target_fields_str} VALUES %s"

            # Add conflict handling if specified
            if conflict_fields:
                conflict_fields_str = "(" + ', '.join(f'"{cf}"' for cf in conflict_fields) + ")"

                if update_fields:
                    # Handle ON CONFLICT DO UPDATE cases
                    if append_fields:
                        # Create update expressions that either append or replace values
                        excluded_fields_str = [
                            f'EXCLUDED."{field}"+{table}."{field}"' if field in append_fields else
                            f'EXCLUDED."{field}"' for field in update_fields
                        ]
                    else:
                        # Create simple update expressions
                        excluded_fields_str = [f'EXCLUDED."{field}"' for field in update_fields]

                    # Format the update fields clause
                    if len(excluded_fields_str) > 1:
                        update_fields_str = (
                            "(" + ', '.join(f'"{uf}"' for uf in update_fields) + ")"
                            + f" = ({', '.join(excluded_fields_str)})"
                        )
                    else:
                        update_fields_str = f'"{update_fields[0]}" = {excluded_fields_str[0]}'

                    # Add the ON CONFLICT DO UPDATE clause
                    sql += f" ON CONFLICT {conflict_fields_str} DO UPDATE SET {update_fields_str}"
                else:
                    # Handle ON CONFLICT DO NOTHING cases
                    sql += f" ON CONFLICT {conflict_fields_str} DO NOTHING"

        return sql, template

    @staticmethod
    def _serialize_cell(cell) -> str | None:
        """
        Serialize a cell value into a string representation.

        :param cell: The value to be serialized.
        :type cell: Any

        :return: The serialized string representation of the cell value, or None if the cell value
            is None.
        :rtype: str or None
        """
        if cell is None:
            return None
        if isinstance(cell, datetime):
            return cell.isoformat()
        return str(cell)

    def execute(self, context: Context) -> None:
        # BigQuery Hook to interact with BigQuery.
        big_query_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )

        # SQL Hook to interact with SQL Database.
        self.persist_links(context)
        sql_hook = self.get_sql_hook()

        # Determine Execution Method
        execution_method = self._determine_execution_method()
        self.log.info(f'Using execution method : {execution_method}')

        # Retrieve batches of records from BigQuery.
        for rows in bigquery_get_data(
                logger=self.log,
                dataset_id=self.dataset_id,
                table_id=self.table_id,
                big_query_hook=big_query_hook,
                batch_size=self.bigquery_batch_size,
                selected_fields=self.selected_fields,
        ):
            total_rows_loaded = 0
            # Open a connection to the SQL database using the SQL hook and ensure it's properly
            # closed afterward.
            with closing(sql_hook.get_conn()) as conn:
                try:
                    if sql_hook.supports_autocommit:
                        sql_hook.set_autocommit(conn, False)
                    conn.commit()

                    # Create a cursor to execute SQL queries using the opened database connection.
                    with closing(conn.cursor()) as cur:
                        for chunked_rows in chunked(rows, self.sql_batch_size):
                            # Serialize each cell in every row of the current chunk and create a list of
                            # tuples containing the serialized values.

                            values = list(
                                map(
                                    lambda row: tuple(
                                        map(lambda cell: self._serialize_cell(cell), row)
                                    ),
                                    chunked_rows
                                )
                            )

                            # Execute INSERT operation using the generated SQL statement and values.
                            if self.database_type.lower() == "postgres":
                                if execution_method == "values":
                                    # Use execute_values with the enhanced SQL generation
                                    sql, template = self._generate_values_sql(
                                        self.target_table_name,
                                        self.selected_fields,
                                        self.replace,
                                        self.update_fields,
                                        self.conflict_fields,
                                        self.append_fields
                                    )
                                    execute_values(
                                        cur,
                                        sql,
                                        values,
                                        template=template,
                                        page_size=self.sql_batch_size
                                    )
                                else:
                                    sql = self._generate_batch_sql(
                                        self.target_table_name,
                                        values[0],
                                        self.selected_fields,
                                        self.replace,
                                        self.update_fields,
                                        self.conflict_fields,
                                        self.append_fields
                                    )
                                    execute_batch(cur, sql, values, page_size=self.sql_batch_size)

                            conn.commit()
                            self.log.info(
                                f"Loaded {len(chunked_rows)} rows into {self.target_table_name} so far"
                            )
                            total_rows_loaded += len(chunked_rows)
                except Exception as ex:
                    error_type = type(ex).__name__
                    error = f"Database error occurred. Type: {error_type}, error: {self.get_db_error(ex)}"
                    self.log.error(error)
                    conn.rollback()
                    # breaking exception chain else taskinstance.py from airflow implicitly logs
                    # stackTrace from original exception instance
                    raise AirflowFailException(f"{error}") from None

            self.log.info(
                f"Done loading. Loaded a total of {total_rows_loaded} "
                f"rows into {self.target_table_name}."
            )
