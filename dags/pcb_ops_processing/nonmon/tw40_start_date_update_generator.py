# for airflow scanning
import logging
from datetime import datetime, timedelta
from airflow.exceptions import AirflowFailException

from pcb_ops_processing.nonmon.abc.base_nonmon_file_generator import (
    BaseNonmonFileGenerator,
    NONMON_BODY_IDENTIFIER,
    HEADER_IDNTFR,
    TRAILER_IDNTFR
)


class TW40StartDateUpdateFileGenerator(BaseNonmonFileGenerator):
    """Concrete implementation for TW40 Start Date update processing."""

    def generate_nonmon_record(self, account_id: str, original_row=None) -> str:
        """
        Generate TW40 Start Date update record in fixed-width format.

        Args:
            account_id: Account identifier
            original_row: CSV row containing account data including open date

        Returns:
            str: Fixed-width record (596 characters)
        """
        if original_row is None or len(original_row) < 3:
            row_info = f"Account ID: {account_id}, Row: {original_row}"
            raise AirflowFailException(
                f"Account data including open date and offset days is required.\n{row_info}"
            )

        # Extract account open date from CSV (assuming it's in the second column)
        # Format expected: YYYY-MM-DD or similar
        try:
            account_open_date_str = original_row[1].strip()
            # Parse the account open date
            if '-' in account_open_date_str:
                account_open_date = datetime.strptime(account_open_date_str, '%Y-%m-%d')
            elif '/' in account_open_date_str:
                account_open_date = datetime.strptime(account_open_date_str, '%Y/%m/%d')
            else:
                # Try other common formats
                account_open_date = datetime.strptime(account_open_date_str, '%Y%m%d')
        except (ValueError, IndexError):
            raise AirflowFailException(
                f"Invalid account open date format: {original_row[1].strip() if len(original_row) > 1 else original_row}")
        try:
            offset_days = int(original_row[2])
        except ValueError:
            raise AirflowFailException(
                f"Account data including open date and offset days is required.\n{original_row}"
            )
        # Calculate TW40 Start Date (Account Open Date + offset_days days)
        tw40_start_date = account_open_date + timedelta(days=offset_days)

        # Convert to Julian date format (YYYYDDD)
        julian_date = tw40_start_date.strftime('%Y%j')

        # Format account ID: Right justified, 'I' filled
        # Example: "00013951111" becomes "IIIIII00013951101"
        account_id_str = str(account_id).zfill(11)

        # Build the TW40 Start Date update record according to specification
        tw40_record = (
            NONMON_BODY_IDENTIFIER  # Record type identifier
            + account_id_str  # Account ID (11 chars)
            + "00"  # Filler
            + "ts2opr".ljust(6)  # Operator ID (20-25) - using ts2opr as specified
            + "245"  # Record Type (26-28) - Tiered Watch code change
            + "245002"  # Field Indicator (29-34)
            + "40"  # Tiered Watch Reason (35-36) - TW40 code
            + "245107"  # Field Indicator (37-42) - Start Date field
            + julian_date.ljust(7)  # Tiered Watch Start Date (43-49) - Julian format
            + "245207"  # Field Indicator (50-55) - End Date field
            + "9999999"  # Tiered Watch Stop Date (56-62) - No change
        )

        return tw40_record.ljust(596)  # Pad to standard record length

    def build_nonmon_table(self, table_id: str) -> str:
        """
        Build BigQuery table DDL for TW40 Start Date update records.

        Args:
            table_id: Target BigQuery table identifier

        Returns:
            str: CREATE TABLE DDL statement
        """
        tw40_ddl = f"""CREATE TABLE IF NOT EXISTS `{table_id}`
                (
                    FILLER_1 STRING,
                    ACCOUNT_NO STRING,
                    FILLER_2 STRING,
                    OPERATOR_ID STRING,
                    RECORD_TYPE STRING,
                    FIELD_INDICATOR_1 STRING,
                    TIERED_WATCH_REASON STRING,
                    FIELD_INDICATOR_2 STRING,
                    TIERED_WATCH_START_DATE STRING,
                    FIELD_INDICATOR_3 STRING,
                    TIERED_WATCH_STOP_DATE STRING,
                    REC_LOAD_TIMESTAMP DATETIME,
                    FILE_NAME STRING
                )
            """
        return tw40_ddl

    def build_nonmon_query(self, transformed_views: list, table_id: str) -> str:
        """
        Build TW40 Start Date update query for inserting data into BigQuery table.

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
            Building query for TW40 Start Date update view: {view_id}
            Record: {record}
            Filename: {filename}
            """)

            # Map columns based on TW40 record structure
            select_query = f"""
                SELECT
                    SUBSTR({record}, 1, 6)    AS FILLER_1,
                    SUBSTR({record}, 7, 11)   AS ACCOUNT_NO,
                    SUBSTR({record}, 18, 2)   AS FILLER_2,
                    SUBSTR({record}, 20, 6)   AS OPERATOR_ID,
                    SUBSTR({record}, 26, 3)   AS RECORD_TYPE,
                    SUBSTR({record}, 29, 6)   AS FIELD_INDICATOR_1,
                    SUBSTR({record}, 35, 2)   AS TIERED_WATCH_REASON,
                    SUBSTR({record}, 37, 6)   AS FIELD_INDICATOR_2,
                    SUBSTR({record}, 43, 7)   AS TIERED_WATCH_START_DATE,
                    SUBSTR({record}, 50, 6)   AS FIELD_INDICATOR_3,
                    SUBSTR({record}, 56, 7)   AS TIERED_WATCH_STOP_DATE,
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

        logger.info("Final combined query for TW40 Start Date update:")
        logger.info(full_query)

        return full_query


globals().update(TW40StartDateUpdateFileGenerator('pcb_ops_pcmc_tw40_start_date_update_dag_config.yaml').create())
