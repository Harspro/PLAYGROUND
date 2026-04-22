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


class Rt159AccountClosureFileGenerator(BaseNonmonFileGenerator):

    def generate_nonmon_record(self, account_id: str, original_row=None) -> str:
        """Generate fixed-width memo record. """
        reason_code = ''
        record_type = ''
        if original_row and len(original_row) >= 2 and original_row[1]:
            reason_code = str(original_row[1]).strip()[:2]
        if not reason_code:
            error_msg = (
                f"Missing reason_code for Account ID: {account_id}, "
                f"Row: {original_row}. Failing DAG."
            )
            raise AirflowFailException(error_msg)

        if original_row and len(original_row) >= 3 and original_row[2]:
            record_type = str(original_row[2]).strip()[:3]
        if not record_type:
            error_msg = (
                f"Missing record_type for Account ID: {account_id}, "
                f"Row: {original_row}. Failing DAG."
            )
            raise AirflowFailException(error_msg)

        mast_id_str = str(account_id).zfill(11)

        # Determine field_ind1 based on record_type
        if record_type == "272":
            # Watch Out records: field_ind1 = 272002
            field_ind1 = "272002"
        elif record_type == "159":
            # Account Closure records (159): field_ind1 = 020702
            field_ind1 = "020702"
        else:
            # Invalid record_type - only 272 and 159 are supported
            error_msg = (
                f"Invalid record_type '{record_type}' for Account ID: {account_id}. "
                f"Only '272' (Watch Out) and '159' (Account Closure) are supported. "
                f"Row: {original_row}. Failing DAG."
            )
            raise AirflowFailException(error_msg)

        record = (
            NONMON_BODY_IDENTIFIER          # 001-006
            + mast_id_str                   # 007-017
            + "00"                          # 018-019
            + TSYS_ID                       # 020-025
            + record_type                   # 026-028
            + field_ind1                    # 029-034
            + reason_code                   # 035-036
            + "272107"                      # 037-042
            + "2025164"                     # 043-049
            + "272207"                      # 050-055
            + "2099001"                     # 056-062
        )

        return record.ljust(596)

    def build_nonmon_table(self, table_id: str) -> str:
        account_closure_ddl = f"""CREATE TABLE IF NOT EXISTS `{table_id}` (
            FILLER_1 STRING,               -- 1-6
            MAST_ACCOUNT_ID STRING,        -- 7-17
            FILLER_2 STRING,               -- 18-19
            TSYSID STRING,                 -- 20-25
            RECORD_TYPE STRING,            -- 26-28
            STATIC_CODE STRING,            -- 29-34
            REASON_CODE STRING,            -- 35-36
            FIELD_IND2 STRING,             -- 37-42
            JULDATE STRING,                -- 43-49
            FIELD_IND3 STRING,             -- 50-55
            STOP_DATE STRING,              -- 56-62
            REC_LOAD_TIMESTAMP DATETIME,
            FILE_NAME STRING
        )"""
        return account_closure_ddl

    def build_nonmon_query(self, transformed_views: list, table_id: str) -> str:
        """
        Build memo query for inserting transformed data into BigQuery table.
        """
        logger = logging.getLogger(__name__)
        selects = []

        for view in transformed_views:
            view_id = view['id']
            record = view['columns'].split(',')[0].strip()
            filename = view['columns'].split(',')[1].strip()

            logger.info(f"""
            Building query for view: {view_id}
            record   :: {record}
            filename :: {filename}
            """)

            selects.append(f"""
                SELECT
                    SUBSTR({record}, 1, 6)    AS FILLER_1,
                    SUBSTR({record}, 7, 11)   AS MAST_ACCOUNT_ID,
                    SUBSTR({record}, 18, 2)   AS FILLER_2,
                    SUBSTR({record}, 20, 6)   AS TSYSID,
                    SUBSTR({record}, 26, 3)   AS RECORD_TYPE,
                    SUBSTR({record}, 29, 6)   AS STATIC_CODE,
                    SUBSTR({record}, 35, 2)   AS REASON_CODE,
                    SUBSTR({record}, 37, 6)   AS FIELD_IND2,
                    SUBSTR({record}, 43, 7)   AS JULDATE,
                    SUBSTR({record}, 50, 6)   AS FIELD_IND3,
                    SUBSTR({record}, 56, 7)   AS STOP_DATE,
                    CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
                    {filename}
                FROM `{view_id}`
                WHERE NOT REGEXP_CONTAINS({record}, r'{HEADER_IDNTFR}|{TRAILER_IDNTFR}')
            """)

        full_query = f"INSERT INTO {table_id}\n" + "\nUNION ALL\n".join(selects)

        logger.info(f"""
        Final combined query for account closure:
        {full_query}
        """)

        return full_query


globals().update(
    Rt159AccountClosureFileGenerator('pcb_ops_account_closure_dag_config.yaml').create()
)
