# for airflow scanning
from airflow import DAG
from airflow.exceptions import AirflowFailException
from datetime import datetime
import logging

from pcb_ops_processing.nonmon.abc.base_nonmon_file_generator import (
    BaseNonmonFileGenerator,
    NONMON_BODY_IDENTIFIER,
    TSYS_ID,
    HEADER_IDNTFR,
    TRAILER_IDNTFR
)


class CardExpirationDateUpdateFileGenerator(BaseNonmonFileGenerator):
    """Concrete implementation for account card expiration update processing."""

    def generate_nonmon_record(self, account_id: str, original_row=None) -> str:
        if original_row is None or not isinstance(original_row, list) or len(original_row) < 2:
            raise AirflowFailException(f"""
                The input row should contain account_id, card_expiration_date.
                Account ID: {account_id}, Row: {original_row}
            """)

        account_id_str = str(account_id).zfill(16)
        card_expiration_julian_date = datetime.strptime(original_row[1].strip(), '%Y-%m-%d').strftime('%Y%j')

        # Placeholder record structure - customize based on requirements
        hierarchy_record = (
            account_id_str  # Account ID (16 chars)
            + "@@@"  # Filler
            + TSYS_ID  # TSYS ID
            + "176"  # Record type
            + "026504"  # Field indicator
            + card_expiration_julian_date  # Card Expiration Date
        )

        return hierarchy_record.ljust(596)  # Pad to standard record length

    def build_nonmon_table(self, table_id: str) -> str:
        hierarchy_ddl = f"""
            CREATE TABLE IF NOT EXISTS `{table_id}`
            (
                ACCOUNT_ID STRING,
                FILLER STRING,
                TSYS_ID STRING,
                RECORD_TYPE STRING,
                FIELD_INDICATOR STRING,
                CARD_EXPIRATION_JULIAN_DATE STRING,
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
                Building query for payment hierarchy view: {view_id}
                record :: {record}
                filename :: {filename}
            """)

            # Customize column mapping based on payment hierarchy record
            select_query = f"""
                SELECT
                    SUBSTR({record}, 1, 16)   AS ACCOUNT_ID,
                    SUBSTR({record}, 17, 3)   AS FILLER,
                    SUBSTR({record}, 20, 6)   AS TSYS_ID,
                    SUBSTR({record}, 26, 3)   AS RECORD_TYPE,
                    SUBSTR({record}, 29, 6)  AS FIELD_INDICATOR,
                    SUBSTR({record}, 35, 7)  AS CARD_EXPIRATION_JULIAN_DATE,
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


globals().update(CardExpirationDateUpdateFileGenerator('pcb_ops_pcmc_card_expiration_date_update_dag_config.yaml').create())
