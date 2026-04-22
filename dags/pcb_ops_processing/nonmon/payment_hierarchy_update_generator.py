# for airflow scanning
from airflow import DAG
from airflow.exceptions import AirflowFailException
import logging

from pcb_ops_processing.nonmon.abc.base_nonmon_file_generator import (
    BaseNonmonFileGenerator,
    NONMON_BODY_IDENTIFIER,
    TSYS_ID,
    HEADER_IDNTFR,
    TRAILER_IDNTFR
)


class PaymentHierarchyUpdateFileGenerator(BaseNonmonFileGenerator):
    """Concrete implementation for payment hierarchy update processing."""

    def generate_nonmon_record(self, account_id: str, original_row=None) -> str:
        if original_row is None:
            row_info = f"Account ID: {account_id}, Row: {original_row}"
            raise AirflowFailException(
                f"Payment hierarchy data is required.\n{row_info}"
            )

        account_id_str = str(account_id).zfill(11)

        # Placeholder record structure - customize based on requirements
        hierarchy_record = (
            NONMON_BODY_IDENTIFIER  # Record type identifier
            + account_id_str  # Account ID (11 chars)
            + "00"  # Filler
            + TSYS_ID  # TSYS ID
            + "231"  # Record type
            + "460001"  # Field indicator
            + "A"  # payment hierarchy code
        )

        return hierarchy_record.ljust(596)  # Pad to standard record length

    def build_nonmon_table(self, table_id: str) -> str:
        hierarchy_ddl = f"""CREATE TABLE IF NOT EXISTS `{table_id}`
                (
                    FILLER_1 STRING,
                    CIFP_ACCOUNT_ID STRING,
                    FILLER_2 STRING,
                    TSYS_ID STRING,
                    RECORD_TYPE STRING,
                    FIELD_INDICATOR STRING,
                    PAYMENT_HIERARCHY_CODE STRING,
                    REC_LOAD_TIMESTAMP DATETIME,
                    FILE_NAME STRING
                )
            """
        return hierarchy_ddl

    def build_nonmon_query(self, transformed_views: list, table_id: str) -> str:
        """
        Build payment hierarchy query for inserting data into BigQuery table.

        Args:
            transformed_views: List of view configurations with columns
            table_id: Target BigQuery table identifier

        Returns:
            str: Complete INSERT query with UNION ALL of all views
        """
        logger = logging.getLogger(__name__)
        select_queries = []  # List to hold individual SELECT statements

        for view in transformed_views:
            view_id = view['id']
            record = view['columns'].split(',')[0].strip()
            filename = view['columns'].split(',')[1].strip()

            logger.info(f"""
            Building query for view: {view_id}
            record   :: {record}
            filename :: {filename}
            """)

            # Customize column mapping based on payment hierarchy record
            select_query = f"""
                SELECT
                    SUBSTR({record}, 1, 6)    AS FILLER_1,
                    SUBSTR({record}, 7, 11)   AS CIFP_ACCOUNT_ID,
                    SUBSTR({record}, 18, 2)   AS FILLER_2,
                    SUBSTR({record}, 20, 6)   AS TSYS_ID,
                    SUBSTR({record}, 26, 3)   AS RECORD_TYPE,
                    SUBSTR({record}, 29, 6)  AS FIELD_INDICATOR,
                    SUBSTR({record}, 35, 1)  AS PAYMENT_HIERARCHY_CODE,
                    CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
                    {filename} AS FILE_NAME
                FROM
                    `{view_id}`
                WHERE NOT REGEXP_CONTAINS({record}, r'{HEADER_IDNTFR}|{TRAILER_IDNTFR}')
            """
            select_queries.append(select_query)

        # Join all SELECT queries with UNION ALL
        union_all_query = "\nUNION ALL\n".join(select_queries)

        # Wrap the UNION ALL query with INSERT INTO
        full_query = f"""
        INSERT INTO {table_id}
        {union_all_query}
        """

        logger.info(f"""
        Final combined query for payment hierarchy:
        {full_query}
        """)

        return full_query


globals().update(PaymentHierarchyUpdateFileGenerator('pcb_ops_pcmc_payment_hierarchy_update_dag_config.yaml').create())
