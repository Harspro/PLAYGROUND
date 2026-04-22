CREATE
OR REPLACE TABLE `{staging_table_id}` OPTIONS (
    expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
) AS (
    SELECT
        DISTINCT PCF_CUST_ID,
        ACCOUNT_NO,
        CARD_NUMBER,
        AMOUNT,
        SEQ_NUM,
        CHEQUE_NUM,
        CHEQUE_DATE,
        PROCESS_DATE,
        TRANSIT_NUMBER,
        INSTITUTION_ID,
        FDR_FLAG,
        FDR_DATE,
        CREATE_DT,
        UPDATE_USER_ID,
        UPDATE_DT,
        EXTRACT(
            MONTH
            FROM
                UPDATE_DT
        ) AS UPDATE_MONTH,
        EXTRACT(
            YEAR
            FROM
                UPDATE_DT
        ) AS UPDATE_YEAR,
        ST_CD,
        ST_DT,
        CSH_IND,
        SAFE_CAST(CREATE_USER_ID AS STRING) AS CREATE_USER_ID,
        SAFE_CAST(CREATE_FUNCTION_NAME AS STRING) AS CREATE_FUNCTION_NAME,
        SAFE_CAST(TRACE_ID AS STRING) AS TRACE_ID,
        SAFE_CAST(UPDATE_FUNCTION_NAME AS STRING) AS UPDATE_FUNCTION_NAME,
        PAPER_PAYMENT_SOURCE,
        CURRENT_DATE('America/Toronto') AS BI_QUERY_PAPER_PAYMENT_DATE,
        '{run_id}' AS BI_QUERY_DAG_RUN_ID,
        SAFE_CAST(ABS(FARM_FINGERPRINT(GENERATE_UUID())) AS STRING) AS BI_QUERY_PAPER_PAYMENT_UID
    FROM
        (
            SELECT
                DISTINCT CI.CUSTOMER_IDENTIFIER_NO AS PCF_CUST_ID,
                A.MAST_ACCOUNT_ID AS ACCOUNT_NO,
                COALESCE(PAPER.ACCESS_MEDIUM_NO, '-1') AS CARD_NUMBER,
                PAPER.AMOUNT AS AMOUNT,
                COALESCE(PAPER.SEQ_NUM, '-1') AS SEQ_NUM,
                PAPER.CHEQUE_NUM AS CHEQUE_NUM,
                PAPER.CHEQUE_DATE AS CHEQUE_DATE,
                DATETIME(PAPER.PROCESS_DATE) AS PROCESS_DATE,
                PAPER.TRANSIT_NUMBER AS TRANSIT_NUMBER,
                PAPER.INSTITUTION_ID AS INSTITUTION_ID,
                PAPER.FDR_FLAG AS FDR_FLAG,
                PAPER.FDR_DATE AS FDR_DATE,
                PAPER.REC_CREATE_TMS AS CREATE_DT,
                PAPER.REC_CHNG_ID AS UPDATE_USER_ID,
                COALESCE(PAPER.REC_CHNG_TMS, '2007-01-01') AS UPDATE_DT,
                PAPER.ST_CD AS ST_CD,
                PAPER.ST_DT AS ST_DT,
                PAPER.CSH_IND AS CSH_IND,
                NULL AS CREATE_USER_ID,
                NULL AS CREATE_FUNCTION_NAME,
                NULL AS TRACE_ID,
                NULL AS UPDATE_FUNCTION_NAME,
                '053 : PAPER_PYMT_TX' AS PAPER_PAYMENT_SOURCE,
                ROW_NUMBER() OVER (
                    PARTITION BY PAPER.ACCESS_MEDIUM_NO,
                    PAPER.SEQ_NUM
                    ORDER BY
                        PAPER.REC_CHNG_TMS DESC
                ) AS REC_RANK
            FROM
                `pcb-{env}-curated.domain_payments.PAPER_PYMT_TX` PAPER
                LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCESS_MEDIUM` AM ON AM.CARD_NUMBER = PAPER.ACCESS_MEDIUM_NO
                LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` AC ON AC.ACCOUNT_CUSTOMER_UID = AM.ACCOUNT_CUSTOMER_UID
                LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCOUNT` A ON A.ACCOUNT_UID = AC.ACCOUNT_UID
                LEFT JOIN `pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` CI ON CI.CUSTOMER_UID = AC.CUSTOMER_UID
            WHERE
                CI.TYPE = "PCF-CUSTOMER-ID"
                OR CI.TYPE IS NULL
        )
    WHERE
        REC_RANK = 1
);