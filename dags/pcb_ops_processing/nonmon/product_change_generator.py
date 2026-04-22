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

# Valid Change Option Sets for card types
VALID_CHANGE_OPTION_SETS = {
    '0004': 'PCB WORLD CHIP/PIN',
    '0005': 'PCB WORLD CHIP/SIG',
    '0006': 'PCB SILVER CHIP/PIN',
    '0007': 'PCB SILVER CHIP/SIG',
}

# Fixed field values per requirements
CARD_DELIVERY = 'N'
PIN_MAILER_TYPE = 'N'  # null/space
ALT_SHIPPING_ADDRESS_IND = 'N'
FIELD_INDICATOR = ['260004', '260102', '260201', '260301', '260401', '260540', '260640', '260719', '260803', '260903', '260013']
FILLER = '@'


class ProductChangeFileGenerator(BaseNonmonFileGenerator):
    """
    Concrete implementation for product change/downgrade processing.

    Used for scenarios like:
    - Downgrading customers from PC Insiders WE to eligible card
    - Record type 260 (product change)
    """

    def _format_account_id(self, account_id: str) -> str:
        """
        Format account ID per TSYS requirements:
        - Right-justified
        - Filled with 'I's on the left
        - '00' suffix at the end

        Example: account_id "2222222217" -> "IIIIII222222221700"
        """
        # Remove any existing padding/suffix
        clean_id = str(account_id).strip()

        # Account ID should be 16 chars total: I-padding + account + "00"
        # If account is 10 digits: IIIIII + 10 digits + 00 = 18 chars
        # Adjust based on actual TSYS spec (typically 16 or 19 chars)
        account_with_suffix = clean_id + "00"

        # Right-justify with 'I' padding to 19 characters
        formatted_id = account_with_suffix.rjust(19, 'I')

        return formatted_id

    def generate_nonmon_record(self, account_id: str, original_row=None) -> str:
        """
        Generate fixed-width nonmon record for product change (type 260).

        Expected CSV columns:
        - account_id: Customer account number
        - change_option_set: Target card type (0004-0007)
        - cycle_day: Current billing cycle day (to be retained)
        """
        if original_row is None or not isinstance(original_row, list) or len(original_row) < 3:
            raise AirflowFailException(f"""
                The input row should contain: account_id, change_option_set, cycle_day.
                Account ID: {account_id}, Row: {original_row}
            """)
        # Extract and validate fields
        change_option_set = str(original_row[1]).strip()
        cycle_day = str(original_row[2]).strip().zfill(2)  # 2-digit cycle day

        # Validate change option set
        if change_option_set not in VALID_CHANGE_OPTION_SETS:
            raise AirflowFailException(f"""
                Invalid change_option_set: {change_option_set}.
                Valid values: {list(VALID_CHANGE_OPTION_SETS.keys())}
                Account ID: {account_id}
            """)

        # Format account ID per TSYS requirements
        formatted_account_id = self._format_account_id(account_id)

        # Build the product change record
        # Adjust positions based on actual TSYS Standard Batch layout
        product_change_record = (
            formatted_account_id.ljust(19)  # Account ID (right-justified with I's, 00 suffix)
            + TSYS_ID.ljust(6)  # TSYS ID (6 chars)
            + "260"  # Record type - Product Change (3 chars)
            + FIELD_INDICATOR[0]
            + change_option_set.ljust(4)  # Change Option Set (4 chars)
            + FIELD_INDICATOR[1]
            + cycle_day.ljust(2)  # Cycle Day - retained (2 chars)
            + FIELD_INDICATOR[2]
            + CARD_DELIVERY.ljust(1)  # Card Delivery: N (1 char)
            + FIELD_INDICATOR[3]
            + PIN_MAILER_TYPE.ljust(1)  # PIN Mailer Type: null (1 char)
            + FIELD_INDICATOR[4]
            + ALT_SHIPPING_ADDRESS_IND.ljust(1)  # Alt Shipping Address Ind: N (1 char)
            + FIELD_INDICATOR[5]
            + FILLER * 40
            + FIELD_INDICATOR[6]
            + FILLER * 40
            + FIELD_INDICATOR[7]
            + FILLER * 19
            + FIELD_INDICATOR[8]
            + FILLER * 3
            + FIELD_INDICATOR[9]
            + FILLER * 3
            + FIELD_INDICATOR[10]
            + FILLER * 13
        )

        return product_change_record.ljust(596)  # Pad to standard record length

    def build_nonmon_table(self, table_id: str) -> str:
        """
        Build BigQuery table DDL for product change records.
        """
        ddl = f"""
            CREATE TABLE IF NOT EXISTS `{table_id}`
            (
                ACCOUNT_ID STRING,
                TSYS_ID STRING,
                RECORD_TYPE STRING,
                FIELD_INDICATOR_1 STRING,
                CHANGE_OPTION_SET STRING,
                FIELD_INDICATOR_2 STRING,
                CYCLE_DAY STRING,
                FIELD_INDICATOR_3 STRING,
                CARD_DELIVERY STRING,
                FIELD_INDICATOR_4 STRING,
                PIN_MAILER_TYPE STRING,
                FIELD_INDICATOR_5 STRING,
                ALT_SHIPPING_ADDRESS_IND STRING,
                FIELD_INDICATOR_6 STRING,
                FILLER_1 STRING,
                FIELD_INDICATOR_7 STRING,
                FILLER_2 STRING,
                FIELD_INDICATOR_8 STRING,
                FILLER_3 STRING,
                FIELD_INDICATOR_9 STRING,
                FILLER_4 STRING,
                FIELD_INDICATOR_10 STRING,
                FILLER_5 STRING,
                FIELD_INDICATOR_11 STRING,
                FILLER_6 STRING,
                REC_LOAD_TIMESTAMP DATETIME,
                FILE_NAME STRING
            )
        """
        return ddl

    def build_nonmon_query(self, transformed_views: list, table_id: str) -> str:
        """
        Build INSERT query to parse product change records into BigQuery.
        """
        logger = logging.getLogger(__name__)
        select_queries = []
        for view in transformed_views:
            view_id = view['id']
            record = view['columns'].split(',')[0].strip()
            filename = view['columns'].split(',')[1].strip()
            logger.info(f"Building query for product change view: {view_id}")

            # Parse fixed-width record using SUBSTR
            # Adjust positions based on actual record layout
            select_query = f"""
                SELECT
                    SUBSTR({record}, 1, 19)   AS ACCOUNT_ID,
                    SUBSTR({record}, 20, 6)   AS TSYS_ID,
                    SUBSTR({record}, 26, 3)   AS RECORD_TYPE,
                    SUBSTR({record}, 29, 6)   AS FIELD_INDICATOR_1,
                    SUBSTR({record}, 35, 4)   AS CHANGE_OPTION_SET,
                    SUBSTR({record}, 39, 6)   AS FIELD_INDICATOR_2,
                    SUBSTR({record}, 45, 2)   AS CYCLE_DAY,
                    SUBSTR({record}, 47, 6)   AS FIELD_INDICATOR_3,
                    SUBSTR({record}, 53, 1)   AS CARD_DELIVERY,
                    SUBSTR({record}, 54, 6)   AS FIELD_INDICATOR_4,
                    SUBSTR({record}, 60, 1)   AS PIN_MAILER_TYPE,
                    SUBSTR({record}, 61, 6)   AS FIELD_INDICATOR_5,
                    SUBSTR({record}, 67, 1)   AS ALT_SHIPPING_ADDRESS_IND,
                    SUBSTR({record}, 68, 6)   AS FIELD_INDICATOR_6,
                    SUBSTR({record}, 74, 40)  AS FILLER_1,
                    SUBSTR({record}, 114, 6)  AS FIELD_INDICATOR_7,
                    SUBSTR({record}, 120, 40) AS FILLER_2,
                    SUBSTR({record}, 160, 6)  AS FIELD_INDICATOR_8,
                    SUBSTR({record}, 166, 19) AS FILLER_3,
                    SUBSTR({record}, 185, 6)  AS FIELD_INDICATOR_9,
                    SUBSTR({record}, 191, 3)  AS FILLER_4,
                    SUBSTR({record}, 194, 6)  AS FIELD_INDICATOR_10,
                    SUBSTR({record}, 200, 3)  AS FILLER_5,
                    SUBSTR({record}, 203, 6)  AS FIELD_INDICATOR_11,
                    SUBSTR({record}, 209, 13) AS FILLER_6,
                    CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
                    {filename} AS FILE_NAME
                FROM
                    `{view_id}`
                WHERE NOT REGEXP_CONTAINS({record}, r'{HEADER_IDNTFR}|{TRAILER_IDNTFR}')
            """
            select_queries.append(select_query)

        # Join all SELECT queries with UNION ALL
        union_all_query = "\nUNION ALL\n".join(select_queries)

        full_query = f"""
        INSERT INTO {table_id}
        {union_all_query}
        """

        logger.info(f"Final combined query for product change: {full_query}")

        return full_query


# Register DAGs with Airflow
globals().update(
    ProductChangeFileGenerator(
        'pcb_ops_pcmc_product_change_dag_config.yaml'
    ).create()
)
