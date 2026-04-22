
from util.miscutils import read_variable_or_file

# Constants
DEPLOY_ENV = read_variable_or_file('gcp_config')['deployment_environment_name']

# Settings
DATETIME_FORMAT_COMBINED = "%Y%m%d%H%M%S"

# Staging tables:
STG_BQTABLE_CARD_OWNER = f"pcb-{DEPLOY_ENV}-processing.domain_aml.STG_ACCOUNT_INFO"
STG_BQTABLE_PRIMARY_ACCOUNT_INFO = f"pcb-{DEPLOY_ENV}-processing.domain_aml.STG_PRIMARY_ACCOUNT_INFO"
STG_BQTABLE_MERCHANT_INFO = f"pcb-{DEPLOY_ENV}-processing.domain_aml.STG_MERCHANT_INFO"

STG_REF_RAIL_WITH_DP_REF_NO = f"pcb-{DEPLOY_ENV}-processing.domain_aml.STG_REF_RAIL_WITH_DP_REF_NO"
STG_REF_RAIL_NO_DP_REF_NO = f"pcb-{DEPLOY_ENV}-processing.domain_aml.STG_REF_RAIL_NO_DP_REF_NO"


# Agg domain tables
AGG_BQTABLE_AT31 = f"pcb-{DEPLOY_ENV}-curated.domain_aml.AGG_AT31"

# Agg control tables
BQTABLE_TRANSACTION_AGG_CONTROL = f"pcb-{DEPLOY_ENV}-processing.domain_aml.TRANSACTION_AGG_CONTROL"

# Select raw tables:
AT31_TRANSACTION_TABLE = f"pcb-{DEPLOY_ENV}-landing.domain_account_management.AT31"
REF_CURRENCY_TABLE = f"pcb-{DEPLOY_ENV}-landing.domain_payments.REF_CURRENCY"


################################################
# Filters
################################################
PURCHASE_FILTER = "AT31_TRANSACTION_CODE = 3001 and AT31_TRANSACTION_CATEGORY = 0001"
RETURNS_FILTER = "AT31_TRANSACTION_CODE=3006 and AT31_TRANSACTION_CATEGORY=0001"
CUSTOMER_INFO_FILTERS = "type = 'PCF-CUSTOMER-ID' AND disabled_ind = 'N'"

################################################
# Select fields
################################################

# Transaction Id (AT31)
TRANSACTION_ID_COLUMN = f"""Coalesce(FORMAT_DATETIME("{DATETIME_FORMAT_COMBINED}", at31.REC_LOAD_TIMESTAMP),'')
|| Coalesce(Cast(AT00_CLIENT_NUM AS String),'')
|| Coalesce(Cast(AT00_APPLICATION_NUM AS String),'')
|| Coalesce(Cast(AT00_APPLICATION_SUFFIX AS String),'')
|| Coalesce(Cast(AT21_CDATE_STMT_BEG AS String),'')
|| Coalesce(FORMAT_TIMESTAMP("%Y%m%d", AT31_DATE_POST),'')
|| Coalesce((CASE
                WHEN
                    LENGTH(SPLIT(AT31_TIME_POST, ':')[OFFSET(0)]) = 1
                THEN
                    CONCAT(0 , SPLIT(AT31_TIME_POST, ':')[OFFSET(0)] )
                ELSE
                    SPLIT(AT31_TIME_POST, ':')[OFFSET(0)]
            END),'')
|| Coalesce((CASE
                WHEN
                    LENGTH(SPLIT(AT31_TIME_POST, ':')[OFFSET(1)]) = 1
                THEN
                    CONCAT(0 , SPLIT(AT31_TIME_POST, ':')[OFFSET(1)] )
                ELSE
                    SPLIT(AT31_TIME_POST, ':')[OFFSET(1)]
            END),'')
|| Coalesce((CASE
                WHEN
                    LENGTH(SPLIT(AT31_TIME_POST, ':')[OFFSET(2)]) = 1
                THEN
                    CONCAT(0 , SPLIT(AT31_TIME_POST, ':')[OFFSET(2)] )
                ELSE
                    SPLIT(AT31_TIME_POST, ':')[OFFSET(2)]
            END),'')
|| Coalesce((CASE
       WHEN AT31_REFERENCE_NUM is not null AND AT31_REFERENCE_NUM <> '' THEN CASE
                WHEN AT31_BANKNET_REF_NUMBER is not null AND AT31_BANKNET_REF_NUMBER <> '' THEN CONCAT('-', AT31_REFERENCE_NUM, '-', AT31_BANKNET_REF_NUMBER)
                ELSE CONCAT('-', AT31_REFERENCE_NUM)
                END
       ELSE CASE
                WHEN AT31_BANKNET_REF_NUMBER is not null AND AT31_BANKNET_REF_NUMBER <> '' THEN CONCAT('-0-', AT31_BANKNET_REF_NUMBER)
                ELSE ''
                END
       END), '')
"""

# Transaction Date (AT31)
AT31_TRANSACTION_DT = """
        FORMAT_DATE("%Y-%m-%d", AT31_DATE_TRANSACTION)
        ||" "
        || (CASE WHEN AT31_CTIME_AUTHORIZATION <> 0 THEN PARSE_TIME(
                "%H%M%E*S",
                LPAD(
                    SAFE_CAST(
                        DIV(AT31_CTIME_AUTHORIZATION -9999999, -10) AS STRING
                    ),
                      6, '0')
            ) ELSE PARSE_TIME("%H%M%E*S", '000000') END
            )"""


# Posted Date
POSTED_DATE_COLUMN = """FORMAT_DATE("%y-%br-%d",AT31_DATE_POST)
|| " "
|| LEFT(AT31_TIME_POST,5)
|| ":"
|| SAFE_CAST(RIGHT(AT31_TIME_POST,3) AS INT64) / 1000 * 60"""


# Process Reference rail table
REFERENCE_TABLE_BQ = f"pcb-{DEPLOY_ENV}-curated.domain_aml.REF_TRANSACTION_RAIL"


def sql_account_info():
    """
    This piece of code gets account info,  -
        - customer identifier
        - customer uid
        - account_no (from table ACCOUNT)
        - account_uid (from table ACCOUNT)
        - access_medium_no (from table ACCESS_MEDIUM)
        - given name and surname
        - product type
    """
    query = f"""
        CREATE OR REPLACE TABLE {STG_BQTABLE_CARD_OWNER} AS
        SELECT X.* EXCEPT (KEY_COUNT)
        FROM (
                SELECT
                    ci.customer_identifier_no    customer_no,
                    ci.customer_uid              customer_uid,
                    a.account_no                 account_no,
                    a.account_uid                account_uid,
                    am.access_medium_no			 access_medium_no,
                    c.given_name                 given_name,
                    c.surname                    surname,
                    p.type                       product_type,
                    ac.ACCOUNT_CUSTOMER_ROLE_UID AS account_customer_role_uid,
                    COUNT(1) OVER (PARTITION BY a.account_no,am.access_medium_no) AS key_count
                FROM
                    (SELECT * FROM `pcb-{DEPLOY_ENV}-landing.domain_customer_management.CUSTOMER_IDENTIFIER` WHERE {CUSTOMER_INFO_FILTERS})  ci
                    LEFT JOIN
                        `pcb-{DEPLOY_ENV}-landing.domain_account_management.ACCOUNT_CUSTOMER`  ac
                    ON ci.customer_uid = ac.customer_uid
                    LEFT JOIN
                        `pcb-{DEPLOY_ENV}-landing.domain_account_management.ACCESS_MEDIUM`     am
                    ON ac.account_customer_uid = am.account_customer_uid
                    LEFT JOIN
                        `pcb-{DEPLOY_ENV}-landing.domain_account_management.ACCOUNT`  a
                    ON a.account_uid = ac.account_uid
                    LEFT JOIN
                        `pcb-{DEPLOY_ENV}-landing.domain_account_management.PRODUCT`  p
                    ON a.product_uid = p.product_uid
                    LEFT JOIN
                        `pcb-{DEPLOY_ENV}-landing.domain_customer_management.CUSTOMER`  c
                    ON ac.customer_uid = c.customer_uid
            )  X
         WHERE X.KEY_COUNT = 1
    """
    return query


def sql_primary_account_info() -> str:
    query = f"""
    CREATE OR REPLACE TABLE {STG_BQTABLE_PRIMARY_ACCOUNT_INFO} AS
    SELECT x.customer_identifier_no AS primary_customer_no
           ,x.account_uid
    FROM
        (SELECT ci.customer_identifier_no
              ,ac.account_uid
              ,ROW_NUMBER() OVER (PARTITION BY account_uid ORDER BY ac.UPDATE_DT DESC) r_rank
        FROM `pcb-{DEPLOY_ENV}-landing.domain_customer_management.CUSTOMER_IDENTIFIER` ci
             INNER JOIN `pcb-{DEPLOY_ENV}-landing.domain_account_management.ACCOUNT_CUSTOMER`  ac
                ON ci.customer_uid = ac.customer_uid
        WHERE ac.ACCOUNT_CUSTOMER_ROLE_UID = 1
             AND ci.TYPE = 'PCF-CUSTOMER-ID'
             AND ci.disabled_ind = 'N'
             AND ac.ACTIVE_IND = 'Y'
        ) x
    WHERE x.r_rank = 1
    """
    return query


def sql_stg_merchant_info() -> str:
    query = f"""
    CREATE OR REPLACE TABLE {STG_BQTABLE_MERCHANT_INFO} AS
    SELECT X.* EXCEPT (KEY_COUNT)
    FROM
       ( SELECT
            rmc.code merchant_cat_code,
            rmcg.description mcc_desc,
            COUNT(1) OVER (PARTITION BY rmc.code) KEY_COUNT
        FROM
          `pcb-{DEPLOY_ENV}-landing.domain_account_management.REF_MERCHANT_CATEGORY` rmc
        INNER JOIN
          `pcb-{DEPLOY_ENV}-landing.domain_account_management.REF_MERCHANT_CATEGORY_GROUP` rmcg
        ON
          rmc.ref_merchant_category_grp_uid = rmcg.ref_merchant_category_grp_uid
       ) X
    WHERE X.KEY_COUNT = 1
    """
    return query


def sql_stg_ref_rail_type_with_dp_ref_no() -> str:
    query = f"""
        CREATE OR REPLACE TABLE {STG_REF_RAIL_WITH_DP_REF_NO} AS
        SELECT
         a.RAIL_TYPE,
         a.INITIATOR,
         a.TRANSACTION_CATEGORY,
         a.TRANSACTION_CD,
         a.DP_REF_NO
       FROM
         {REFERENCE_TABLE_BQ} a INNER JOIN (
                                              SELECT TRANSACTION_CATEGORY,
                                                     TRANSACTION_CD,
                                                     DP_REF_NO
                                              FROM {REFERENCE_TABLE_BQ}
                                              WHERE   DP_REF_NO IS NOT NULL
                                              GROUP BY TRANSACTION_CATEGORY,TRANSACTION_CD,DP_REF_NO
                                              HAVING count(1) = 1
                                            ) b ON a.TRANSACTION_CATEGORY = b.TRANSACTION_CATEGORY
                                                 AND  a.TRANSACTION_CD = b.TRANSACTION_CD
                                                 AND  a.DP_REF_NO = b.DP_REF_NO
       WHERE a.DP_REF_NO IS NOT NULL
    """
    return query


def sql_stg_ref_rail_type_no_dp_ref_no() -> str:
    query = f"""
        CREATE OR REPLACE TABLE {STG_REF_RAIL_NO_DP_REF_NO} AS
        SELECT
         a.RAIL_TYPE,
         a.INITIATOR,
         a.TRANSACTION_CATEGORY,
         a.TRANSACTION_CD
       FROM
         {REFERENCE_TABLE_BQ} a INNER JOIN (
                                              SELECT TRANSACTION_CATEGORY,
                                                     TRANSACTION_CD
                                              FROM {REFERENCE_TABLE_BQ}
                                              WHERE DP_REF_NO IS NULL
                                              GROUP BY TRANSACTION_CATEGORY,TRANSACTION_CD
                                              HAVING COUNT(1) = 1
                                            ) b ON a.TRANSACTION_CATEGORY = b.TRANSACTION_CATEGORY
                                                 AND  a.TRANSACTION_CD = b.TRANSACTION_CD
       WHERE a.DP_REF_NO IS NULL
    """
    return query


def sql_delete_agg_at31() -> str:
    query = f"""
        DELETE
         FROM {AGG_BQTABLE_AT31} at31
         WHERE  at31.BUSINESS_DT IN UNNEST(@file_create_dt_list)
    """
    return query


def sql_process_agg_at31() -> str:
    """
    Returns the query to create the transaction table for a given feed
        - posp (purchase) or posr (returns)
    """
    query = f"""
         INSERT INTO `{AGG_BQTABLE_AT31}`(
            TRANSACTION_ID,
            CLIENT_NO,
            APPLICATION_NO,
            APPLICATION_SUFFIX,
            BANKNET_REF_NO,
            REF_NO,
            ACCOUNT_NO,
            CARD_NO,
            PRODUCT_TYPE,
            CUSTOMER_NO,
            CUSTOMER_FIRST_NAME,
            CUSTOMER_LAST_NAME,
            PRIMARY_CUSTOMER_NO,
            TRANSACTION_DT,
            POSTED_DT,
            FINAL_AMT,
            ORIGINAL_AMT,
            CURRENCY_CD,
            FOREIGN_EXCHANGE_RATE,
            TRANSACTION_CATEGORY,
            TRANSACTION_CD,
            TRANSACTION_DESC,
            TRANSACTION_DEBIT_CREDIT_IND,
            MERCHANT_CATEGORY_CD,
            MERCHANT_CATEGORY_CD_DESC,
            MERCHANT_NAME,
            AUTHORIZATION_CD,
            REVERSE_IND,
            REC_LOAD_TIMESTAMP,
            RAIL_TYPE,
            INITIATOR,
            MERCHANT_TERMINAL_ID,
            MERCHANT_ZIP_CD,
            MERCHANT_CITY,
            MERCHANT_STATE,
            MERCHANT_COUNTRY,
            POS_ENTRY_MODE,
            CLASS_CD,
            TRANSACTION_ACCOUNT_FUNCTION,
            TRANSACTION_SOURCE,
            TERMS_BALANCE_CD,
            BUSINESS_DT,
            RUN_ID,
            CREATE_FUNCTION_NAME
        )
    SELECT
        ({TRANSACTION_ID_COLUMN}) AS TRANSACTION_ID,
        at00_client_num AS CLIENT_NO,
        at00_application_num AS APPLICATION_NO,
        at00_application_suffix AS APPLICATION_SUFFIX,
        at31_banknet_ref_number AS BANKNET_REF_NO,
        at31_reference_num AS REF_NO,
        LPAD(CAST(mast_account_id AS String),11,'0') AS ACCOUNT_NO,
        at31_account_numb AS CARD_NO,
        card_owner.product_type as PRODUCT_TYPE,
        card_owner.customer_no as CUSTOMER_NO,
        card_owner.given_name As CUSTOMER_FIRST_NAME,
        card_owner.surname As CUSTOMER_LAST_NAME,
        primary_customer.primary_customer_no As PRIMARY_CUSTOMER_NO,
        {AT31_TRANSACTION_DT} AS TRANSACTION_DT,
        AT31_DATE_POST AS POSTED_DT ,
        AT31_AMT_TRANSACTION AS FINAL_AMT,
        AT31_AMT_TRANSACTION_ORIGINAL/100 AS ORIGINAL_AMT,
        ref_currency.ALPHABETIC_CODE AS CURRENCY_CD,
        AT31_NEW_CONV_PLUS_MARKUP_RATE AS FOREIGN_EXCHANGE_RATE,
        LPAD(CAST(AT31.AT31_TRANSACTION_CATEGORY AS String),4,'0') AS TRANSACTION_CATEGORY,
        LPAD(CAST(AT31.AT31_TRANSACTION_CODE AS String),4,'0') AS TRANSACTION_CD,
        AT31_TRANSACTION_DESCRIPTION AS TRANSACTION_DESC,
        AT31_DEBIT_CREDIT_INDICATOR AS TRANSACTION_DEBIT_CREDIT_IND,
        AT31_MERCHANT_CATEGORY_CODE AS MERCHANT_CATEGORY_CD,
        merchant_info.mcc_desc AS MERCHANT_CATEGORY_CD_DESC,
        AT31_MERCHANT_DBA_NAME AS MERCHANT_NAME,
        (CASE WHEN AT31_AUTHORIZATION_CODE = '' THEN NULL ELSE AT31_AUTHORIZATION_CODE END) AS AUTHORIZATION_CD,
        AT31_REVERSE_INDICATOR AS REVERSE_IND,
        at31.REC_LOAD_TIMESTAMP,
        COALESCE(ref_rail_with_dp.RAIL_TYPE,COALESCE(ref_rail_no_dp.RAIL_TYPE,'NA')) AS RAIL_TYPE,
        COALESCE(ref_rail_with_dp.INITIATOR,COALESCE(ref_rail_no_dp.INITIATOR,'NA')) AS INITIATOR,
        (CASE
            WHEN AT31_MERCHANT_ID = '' AND AT31_MRCH_TERMINAL_ID = '' THEN NULL
            WHEN AT31_MERCHANT_ID != '' AND AT31_MRCH_TERMINAL_ID = '' THEN CONCAT(AT31_MERCHANT_ID)
            WHEN AT31_MERCHANT_ID = '' AND AT31_MRCH_TERMINAL_ID != '' THEN CONCAT(AT31_MRCH_TERMINAL_ID)
            ELSE CONCAT(AT31_MERCHANT_ID, "-", AT31_MRCH_TERMINAL_ID)
        END) AS MERCHANT_TERMINAL_ID,
        (CASE WHEN AT31_MERCHANT_ZIP = '' THEN NULL ELSE AT31_MERCHANT_ZIP END) AS MERCHANT_ZIP_CD,
        (CASE WHEN AT31_MERCHANT_DBA_CITY = '' THEN NULL ELSE AT31_MERCHANT_DBA_CITY END) AS MERCHANT_CITY,
        (CASE WHEN AT31_MERCHANT_DBA_STATE = '' THEN NULL ELSE AT31_MERCHANT_DBA_STATE END) AS MERCHANT_STATE,
        (CASE WHEN AT31_MERCHANT_DBA_COUNTRY = '' THEN NULL ELSE AT31_MERCHANT_DBA_COUNTRY END) AS MERCHANT_COUNTRY,
        (CASE WHEN AT31_TERMINAL_ENTRY = '' THEN NULL ELSE AT31_TERMINAL_ENTRY END) AS POS_ENTRY_MODE,
        AT31_CLASS_CODE AS CLASS_CD,
        AT31_TRNS_ACCT_FUNC AS TRANSACTION_ACCOUNT_FUNCTION,
        AT31_TRANSACTION_SOURCE AS TRANSACTION_SOURCE,
        AT31_TERMS_BALANCE_CODE AS TERMS_BALANCE_CD,
        at31.file_create_dt,
        @dag_run_id,
        'aml-tsys-transaction-aggregate'
    FROM {AT31_TRANSACTION_TABLE} AS at31
    LEFT JOIN
        {STG_BQTABLE_MERCHANT_INFO} AS merchant_info
        ON Cast(at31.AT31_MERCHANT_CATEGORY_CODE AS String) =  merchant_info.merchant_cat_code
    LEFT OUTER JOIN
        {REF_CURRENCY_TABLE} ref_currency
        ON at31.AT31_CURRENCY_CODE = ref_currency.NUMERIC_CODE
    LEFT JOIN
        {STG_BQTABLE_CARD_OWNER} AS card_owner
        ON LPAD(CAST(at31.MAST_ACCOUNT_ID AS String),11,'0') = card_owner.account_no
        AND at31.AT31_ACCOUNT_NUMB = card_owner.access_medium_no
    LEFT JOIN
        {STG_BQTABLE_PRIMARY_ACCOUNT_INFO} AS primary_customer
    ON  primary_customer.account_uid = card_owner.account_uid
    LEFT OUTER JOIN
        {STG_REF_RAIL_NO_DP_REF_NO} AS ref_rail_no_dp
        ON  ref_rail_no_dp.TRANSACTION_CATEGORY = LPAD(CAST(at31.AT31_TRANSACTION_CATEGORY AS String),4,'0') AND
            ref_rail_no_dp.TRANSACTION_CD = LPAD(CAST(at31.AT31_TRANSACTION_CODE AS String), 4, '0')
    LEFT OUTER JOIN
        {STG_REF_RAIL_WITH_DP_REF_NO} AS ref_rail_with_dp
        ON  ref_rail_with_dp.TRANSACTION_CATEGORY = LPAD(CAST(at31.AT31_TRANSACTION_CATEGORY AS String),4,'0') AND
            ref_rail_with_dp.TRANSACTION_CD = LPAD(CAST(at31.AT31_TRANSACTION_CODE AS String), 4, '0')    AND
            ref_rail_with_dp.DP_REF_NO = SUBSTR(at31.AT31_REFERENCE_NUM,12,2)
    INNER JOIN {BQTABLE_TRANSACTION_AGG_CONTROL} C on C.FILE_CREATE_DT = at31.FILE_CREATE_DT
                                                                          AND C.REC_LOAD_TIMESTAMP = at31.REC_LOAD_TIMESTAMP
                                                                          AND C.LATEST_VERSION = 'Y'
     WHERE C.RUN_ID = @dag_run_id
         AND at31.FILE_CREATE_DT IN UNNEST(@file_create_dt_list)

    """
    return query


def sql_create_agg_domain_at31_table() -> str:
    sql = f"""CREATE TABLE IF NOT EXISTS {AGG_BQTABLE_AT31}
        (
            TRANSACTION_ID STRING,
            CLIENT_NO NUMERIC,
            APPLICATION_NO NUMERIC,
            APPLICATION_SUFFIX NUMERIC,
            BANKNET_REF_NO STRING,
            REF_NO STRING,
            ACCOUNT_NO STRING,
            CARD_NO STRING,
            PRODUCT_TYPE STRING,
            CUSTOMER_NO STRING,
            CUSTOMER_FIRST_NAME STRING,
            CUSTOMER_LAST_NAME STRING,
            PRIMARY_CUSTOMER_NO STRING,
            TRANSACTION_DT STRING,
            POSTED_DT DATETIME,
            FINAL_AMT NUMERIC,
            ORIGINAL_AMT NUMERIC,
            CURRENCY_CD STRING,
            FOREIGN_EXCHANGE_RATE NUMERIC,
            TRANSACTION_CATEGORY STRING,
            TRANSACTION_CD STRING,
            TRANSACTION_DESC STRING,
            TRANSACTION_DEBIT_CREDIT_IND STRING,
            MERCHANT_CATEGORY_CD NUMERIC,
            MERCHANT_CATEGORY_CD_DESC STRING,
            MERCHANT_NAME STRING,
            AUTHORIZATION_CD STRING,
            REVERSE_IND STRING,
            REC_LOAD_TIMESTAMP DATETIME,
            RAIL_TYPE STRING,
            INITIATOR STRING,
            MERCHANT_TERMINAL_ID STRING,
            MERCHANT_ZIP_CD STRING,
            MERCHANT_CITY STRING,
            MERCHANT_STATE STRING,
            MERCHANT_COUNTRY STRING,
            POS_ENTRY_MODE STRING,
            CLASS_CD STRING,
            TRANSACTION_ACCOUNT_FUNCTION STRING,
            TRANSACTION_SOURCE STRING,
            TERMS_BALANCE_CD NUMERIC,
            BUSINESS_DT DATE,
            RUN_ID STRING,
            CREATE_FUNCTION_NAME STRING
        )
        PARTITION BY DATETIME_TRUNC(REC_LOAD_TIMESTAMP, DAY)
        CLUSTER BY RAIL_TYPE
        """
    return sql


def sql_create_control_table():
    sql = f"""CREATE TABLE IF NOT EXISTS {BQTABLE_TRANSACTION_AGG_CONTROL} (
              RUN_ID STRING,
              FILE_CREATE_DT DATE,
              REC_LOAD_TIMESTAMP DATETIME,
              JOB_START_TIME DATETIME,
              JOB_END_TIME DATETIME,
              STATUS STRING,
              LATEST_VERSION STRING
              )"""
    return sql


"""
Steps (Airflow):
Run the following in parallel
- [ card_owner ,  primary customer table,  merchant_info_table]
- Create the at31 with the date range (and do truncate and insert)
"""
