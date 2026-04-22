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


class AppMemoFileGenerator(BaseNonmonFileGenerator):
    """Concrete implementation for memo processing."""

    def generate_nonmon_record(self, account_id: str, original_row=None) -> str:
        """
        Generate fixed-width memo record.
        Raise error if there are more than 5 memo texts.
        """
        memo_texts = original_row[1:] if original_row else []
        if len(memo_texts) > 5:
            row_info = f"Account ID: {account_id}, Row: {original_row}"
            error_msg = (
                f"Too many memo texts provided ({len(memo_texts)}). "
                f"Maximum allowed is 5.\n{row_info}"
            )
            raise AirflowFailException(error_msg)

        # Pad memo_texts to exactly 5 elements, filling missing ones
        # with empty strings
        padded_memo_texts = (memo_texts + [''] * 5)[:5]

        mast_id_str = str(account_id).zfill(11)

        memo = (
            NONMON_BODY_IDENTIFIER
            + mast_id_str
            + "00"
            + TSYS_ID
            + "910"
            + "953179" + padded_memo_texts[0].ljust(79)[:79]
            + "953279" + padded_memo_texts[1].ljust(79)[:79]
            + "953379" + padded_memo_texts[2].ljust(79)[:79]
            + "953479" + padded_memo_texts[3].ljust(79)[:79]
            + "953579" + padded_memo_texts[4].ljust(79)[:79]
        )
        return memo.ljust(596)

    def build_nonmon_table(self, table_id: str) -> str:
        memo_ddl = f""" CREATE TABLE IF NOT EXISTS `{table_id}`
                (
                    FILLER_1 STRING,
                    CIFP_ACCOUNT_ID STRING,
                    FILLER_2 STRING,
                    TSYSID STRING,
                    RECORD_TYPE STRING,
                    MEMO_1_ID STRING,
                    MEMO_1 STRING,
                    MEMO_2_ID STRING,
                    MEMO_2 STRING,
                    MEMO_3_ID STRING,
                    MEMO_3 STRING,
                    MEMO_4_ID STRING,
                    MEMO_4 STRING,
                    MEMO_5_ID STRING,
                    MEMO_5 STRING,
                    REC_LOAD_TIMESTAMP DATETIME,
                    FILE_NAME STRING
                )
            """
        return memo_ddl

    def build_nonmon_query(self, transformed_views: list, table_id: str) -> str:
        """
        Build memo query for inserting transformed data into BigQuery table.
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

            select_query = f"""
                SELECT
                    SUBSTR({record}, 1, 6)    AS FILLER_1,
                    SUBSTR({record}, 7, 11)   AS CIFP_ACCOUNT_ID,
                    SUBSTR({record}, 18, 2)   AS FILLER_2,
                    SUBSTR({record}, 20, 6)   AS TSYSID,
                    SUBSTR({record}, 26, 3)   AS RECORD_TYPE,
                    SUBSTR({record}, 29, 6)   AS MEMO_1_ID,
                    SUBSTR({record}, 35, 79)  AS MEMO_1,
                    SUBSTR({record}, 114, 6)  AS MEMO_2_ID,
                    SUBSTR({record}, 120, 79) AS MEMO_2,
                    SUBSTR({record}, 199, 6)  AS MEMO_3_ID,
                    SUBSTR({record}, 205, 79) AS MEMO_3,
                    SUBSTR({record}, 284, 6)  AS MEMO_4_ID,
                    SUBSTR({record}, 290, 79) AS MEMO_4,
                    SUBSTR({record}, 369, 6)  AS MEMO_5_ID,
                    SUBSTR({record}, 375, 79) AS MEMO_5,
                    CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
                    {filename}
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
        Final combined query for memo:
        {full_query}
        """)

        return full_query


globals().update(AppMemoFileGenerator('pcb_ops_app_memo_dag_config.yaml').create())
