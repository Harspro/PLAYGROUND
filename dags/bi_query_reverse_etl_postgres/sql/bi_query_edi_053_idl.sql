CREATE
OR REPLACE TABLE `{staging_table_id}` OPTIONS (
    expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
) AS (
    SELECT
        DISTINCT PCF_CUST_ID,
        ACCOUNT_NO,
        CARD_NUMBER,
        PAYER_NAME,
        AMOUNT,
        ORIG_FI_DT,
        EFF_PYMT_DT,
        TRACE_ID,
        FI_TRACE_NUM,
        PAYMENT_TRACE_NO,
        INSTITUTION_ID,
        CREATE_DT,
        HDR_ID,
        EDI_PYMT_UID,
        CARD_DEPOSIT_FULFILLMENT_UID,
        ORIG_CO_SUPP_CD,
        CREATE_USER_ID,
        CREATE_FUNCTION_NAME,
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
        UPDATE_USER_ID,
        UPDATE_FUNCTION_NAME,
        EDI_SOURCE,
        CURRENT_DATE('America/Toronto') AS BI_QUERY_EDI_DATE,
        '{run_id}' AS BI_QUERY_DAG_RUN_ID,
        SAFE_CAST(ABS(FARM_FINGERPRINT(GENERATE_UUID())) AS STRING) AS BI_QUERY_EDI_UID
    FROM
        (
            SELECT
                DISTINCT CI.CUSTOMER_IDENTIFIER_NO AS PCF_CUST_ID,
                A.MAST_ACCOUNT_ID AS ACCOUNT_NO,
                COALESCE(EDI.ACCT_NUM, '-1') AS CARD_NUMBER,
                EDI.CUST_NM AS PAYER_NAME,
                EDI.AMT AS AMOUNT,
                EDI.ORIG_FI_DT AS ORIG_FI_DT,
                EDI.EFF_ENTRY_DT AS EFF_PYMT_DT,
                COALESCE(EDI.TRACE_NUM, '-1') AS TRACE_ID,
                SAFE_CAST(NULL AS STRING) AS FI_TRACE_NUM,
                SAFE_CAST(NULL AS STRING) AS PAYMENT_TRACE_NO,
                EDI.BANK_ID AS INSTITUTION_ID,
                EDI.REC_CREATE_TMS AS CREATE_DT,
                COALESCE(EDI.HDR_ID, -1) AS HDR_ID,
                NULL AS EDI_PYMT_UID,
                NULL AS CARD_DEPOSIT_FULFILLMENT_UID,
                EDI.ORIG_CO_SUPP_CD AS ORIG_CO_SUPP_CD,
                EDI.REC_CHNG_ID AS CREATE_USER_ID,
                SAFE_CAST(NULL AS STRING) AS CREATE_FUNCTION_NAME,
                COALESCE(EDI.REC_CHNG_TMS, '2007-01-01') AS UPDATE_DT,
                SAFE_CAST(NULL AS STRING) AS UPDATE_USER_ID,
                SAFE_CAST(NULL AS STRING) AS UPDATE_FUNCTION_NAME,
                '053 : EDI_PYMT_TRANS' AS EDI_SOURCE,
                ROW_NUMBER() OVER (
                    PARTITION BY EDI.ACCT_NUM,
                    EDI.TRACE_NUM,
                    EDI.HDR_ID
                    ORDER BY
                        EDI.REC_CHNG_TMS DESC
                ) AS REC_RANK
            FROM
                `pcb-{env}-curated.domain_payments.EDI_PYMT_TRANS` EDI
                LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCESS_MEDIUM` AM ON AM.CARD_NUMBER = EDI.ACCT_NUM
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