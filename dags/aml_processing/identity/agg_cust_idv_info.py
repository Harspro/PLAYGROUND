from util.miscutils import read_variable_or_file

# CONSTANTS
DEPLOY_ENV = read_variable_or_file('gcp_config')['deployment_environment_name']

# COTS_AML_SAS RAW TABLES
FSC_PARTY_IDV_INFO_TABLE = f"pcb-{DEPLOY_ENV}-landing.cots_aml_sas.FSC_PARTY_IDV_INFO"
FSC_PARTY_IDV_NFTF_INFO_TABLE = f"pcb-{DEPLOY_ENV}-landing.cots_aml_sas.FSC_PARTY_IDV_NFTF_INFO"

# AGG DOMAIN TABLES
AGG_CUST_IDV_INFO = f"pcb-{DEPLOY_ENV}-curated.domain_aml.CUST_IDV_INFO"

deduplicate_condition = f"""
                        WHERE
                            PARTY_NUMBER NOT IN(
                                                SELECT DISTINCT
                                                    I.CUSTOMER_NUMBER
                                                FROM
                                                    `{AGG_CUST_IDV_INFO}` I
                                                )
                        """


def sql_check_for_duplicates_in_source_tables():
    query = f"""
                WITH
                    DUP_IN_FIRST_TABLE AS
                    (
                        SELECT
                            COUNT(PARTY_NUMBER) AS DUP1
                        FROM
                            {FSC_PARTY_IDV_INFO_TABLE}
                        GROUP BY
                            PARTY_NUMBER
                        HAVING
                            COUNT(PARTY_NUMBER) > 1
                    ),
                    DUP_IN_SECOND_TABLE AS
                    (
                        SELECT
                            COUNT(PARTY_NUMBER) AS DUP2
                        FROM
                            {FSC_PARTY_IDV_NFTF_INFO_TABLE}
                        GROUP BY
                            PARTY_NUMBER
                        HAVING
                            COUNT(PARTY_NUMBER) > 1
                    )
                    SELECT
                        DUP_IN_FIRST_TABLE.DUP1 + DUP_IN_SECOND_TABLE.DUP2
                    FROM
                        DUP_IN_FIRST_TABLE,
                        DUP_IN_SECOND_TABLE
            """
    return query


def sql_insert_from_fsc_agg_cust_idv_info() -> str:
    """
    Returns the query to create the aggregate table for CUST_IDV_INFO
    """
    query = f"""
                INSERT INTO `{AGG_CUST_IDV_INFO}`
                (
                    CUSTOMER_NUMBER,
                    ID_TYPE,
                    ID_TYPE_DESCRIPTION,
                    ID_STATUS,
                    ID_NUMBER,
                    ID_STATE,
                    ID_COUNTRY,
                    ID_ISSUE_DATE,
                    ID_EXPIRY_DATE,
                    NAME_ON_SOURCE,
                    REF_NUMBER,
                    DATE_VERIFIED,
                    TYPE_OF_INFO,
                    IDV_METHOD,
                    IDV_DECISION,
                    CREATE_DT
                )
                SELECT
                    PARTY_NUMBER                                                AS CUSTOMER_NUMBER,
                    ID_TYPE                                                     AS ID_TYPE,
                    ID_TYPE                                                     AS ID_TYPE_DESCRIPTION,
                    'Primary'                                                   AS ID_STATUS,
                    ID_NUMBER,
                    JURISDICTION                                                AS ID_STATE,
                    NATIONALITY                                                 AS ID_COUNTRY,
                    EXTRACT(DATE FROM ISSUE_DATE)                               AS ID_ISSUE_DATE,
                    EXTRACT(DATE FROM EXPIRY_DATE)                              AS ID_EXPIRY_DATE,
                    NULL                                                        AS NAME_ON_SOURCE,
                    NULL                                                        AS REF_NUMBER,
                    NULL                                                        AS DATE_VERIFIED,
                    NULL                                                        AS TYPE_OF_INFO,
                    ID_METHOD                                                   AS IDV_METHOD,
                    ID_METHOD                                                   AS IDV_DECISION,
                    CURRENT_DATETIME()                                          AS CREATE_DT
                FROM
                    `{FSC_PARTY_IDV_INFO_TABLE}`
                {deduplicate_condition}
            """
    return query


def sql_insert_from_nftf_agg_cust_idv_info() -> str:
    query = f"""
                INSERT INTO `{AGG_CUST_IDV_INFO}`
                (
                    CUSTOMER_NUMBER,
                    ID_TYPE,
                    ID_TYPE_DESCRIPTION,
                    ID_STATUS,
                    ID_NUMBER,
                    ID_STATE,
                    ID_COUNTRY,
                    ID_ISSUE_DATE,
                    ID_EXPIRY_DATE,
                    NAME_ON_SOURCE,
                    REF_NUMBER,
                    DATE_VERIFIED,
                    TYPE_OF_INFO,
                    IDV_METHOD,
                    IDV_DECISION,
                    CREATE_DT
                )
                SELECT
                    PARTY_NUMBER                                                        AS CUSTOMER_NUMBER,
                    SOURCE_NAME_1                                                       AS ID_TYPE,
                    SOURCE_NAME_1                                                       AS ID_TYPE_DESCRIPTION,
                    'Primary'                                                           AS ID_STATUS,
                    NULL                                                                AS ID_NUMBER,
                    NULL                                                                AS ID_STATE,
                    'CA'                                                                AS ID_COUNTRY,
                    NULL                                                                AS ID_ISSUE_DATE,
                    NULL                                                                AS ID_EXPIRY_DATE,
                    COALESCE(SOURCE_NAME_1 , BUREAU)                                    AS NAME_ON_SOURCE,
                    REF_NUMBER,
                    SAFE.PARSE_DATE('%Y%m%d' , SAFE_CAST(DATE_VERIFY AS STRING))        AS DATE_VERIFIED,
                    TYPE_OF_INFO_1                                                      AS TYPE_OF_INFO,
                    IDV_METHOD,
                    IDV_DECISION,
                    CURRENT_DATETIME()                                                  AS CREATE_DT
                FROM
                    `{FSC_PARTY_IDV_NFTF_INFO_TABLE}`
                {deduplicate_condition}
            """
    return query


def sql_drop_agg_cust_idv_info_table() -> str:
    sql = f"""
            DROP TABLE IF EXISTS {AGG_CUST_IDV_INFO}
            """
    return sql


def sql_create_agg_cust_idv_info_table() -> str:
    sql = f"""CREATE TABLE IF NOT EXISTS {AGG_CUST_IDV_INFO}
                (
                    CUSTOMER_NUMBER         STRING,
                    ID_TYPE                 STRING,
                    ID_TYPE_DESCRIPTION     STRING,
                    ID_STATUS               STRING,
                    ID_NUMBER               STRING,
                    ID_STATE                STRING,
                    ID_COUNTRY              STRING,
                    ID_ISSUE_DATE           DATE,
                    ID_EXPIRY_DATE          DATE,
                    NAME_ON_SOURCE          STRING,
                    REF_NUMBER              STRING,
                    DATE_VERIFIED           DATE,
                    TYPE_OF_INFO            STRING,
                    IDV_METHOD              STRING,
                    IDV_DECISION            STRING,
                    CREATE_DT               DATETIME
                )
            """
    return sql
