"""
PCF operator for BigQuery to Postgres operations.

Author: Sharif Mansour
"""
from __future__ import annotations

from deprecated import deprecated
from pcf_operators.bigquery_operators.bigquery_to_sql import PcfBigQueryToSqlBaseOperator
from typing import TYPE_CHECKING
from contextlib import closing
from airflow.providers.postgres.hooks.postgres import PostgresHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class PcfBigQueryToPostgresOperator(PcfBigQueryToSqlBaseOperator):
    """
    This is an extension of the BigQueryToPostgresOperator, the current operator performs poorly
    when executing insert statements. Approximately a rate of 100 records per second, the
    improvements to the PcfBigQueryToSqlBaseOperator (parent class) increase the execution rate
    to approximately 10,000 records per second based on early testing.

    The operator is responsible for fetching data from a BigQuery table (alternatively fetch
    selected columns) and inserting it into an PostgreSQL table.

    It handles the db error by reading the stack trace and stripping only the error section.
    when postgres driver throws error it returns below
    ERROR: Unique Constraint error found
    DETAIL|CONTEXT:HINT: on (colA,colB) VALUES ('11','22')

    :param database_type: Database type to insert records into.
    :type database_type: str, default "postgres"

    :param postgres_conn_id: Airflow Postgres Connection ID.
    :type postgres_conn_id: str, default "postgres_default"
    """

    def __init__(
            self,
            *,
            dataset_table: str | None = None,
            target_table_name: str | None = None,
            database_type: str = "postgres",
            postgres_conn_id: str = "postgres_default",
            **kwargs,
    ) -> None:
        self.validate_etl_parameters(dataset_table, target_table_name)
        super().__init__(
            dataset_table=dataset_table,
            target_table_name=target_table_name,
            database_type=database_type,
            **kwargs
        )
        self.postgres_conn_id = postgres_conn_id

    def get_sql_hook(self) -> PostgresHook:
        return PostgresHook(schema=self.database, postgres_conn_id=self.postgres_conn_id)

    def validate_etl_parameters(self, dataset_table: str | None, target_table_name: str | None) -> None:
        if self.__class__.__name__ == "PcfBigQueryToPostgresOperator":
            if dataset_table is None:
                raise ValueError("dataset_table is required for BigQuery to Postgres operations")
            if target_table_name is None:
                raise ValueError("target_table_name is required for BigQuery to Postgres operations")

    def get_db_error(self, ex: Exception) -> dict:
        error = {}
        if hasattr(ex, "pgcode"):
            error['SQLSTATE'] = ex.pgcode
        if hasattr(ex, 'pgerror'):
            error['PG_ERROR'] = self._error(ex.pgerror)
        return error

    @staticmethod
    def _error(error: str):
        for line in error.splitlines():
            if line.startswith("ERROR:"):
                return line.strip()
        return "No Error description found"


@deprecated("This class is deprecated as the pre and post ETL functionality is not required")
class PcfPrePostEtlOperator(PcfBigQueryToPostgresOperator):
    """
    Operator to execute pre and post ETL operations like table creation.
    Should be run before or after the main ETL operator.
    """

    def __init__(
            self,
            *,
            query: str | None = None,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.query = query

    def execute(self, context: Context) -> None:
        """Execute pre post ETL operations."""
        self.log.info(f'Pre Post ETL query : {self.query}')
        if not self.query:
            return

        self.persist_links(context)
        sql_hook = self.get_sql_hook()

        with closing(sql_hook.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                self.log.info("Executing pre post ETL operations...")
                for statement in self.query.split(';'):
                    if statement.strip():
                        cur.execute(statement)
                conn.commit()
