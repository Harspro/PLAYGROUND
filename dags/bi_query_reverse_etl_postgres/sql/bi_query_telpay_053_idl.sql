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
        REPORT_NUMBER,
        SAFE_CAST(RUN_NUMBER AS STRING) AS RUN_NUMBER,
        TELPAY_PYMT_UID,
        CARD_DEPOSIT_FULFILLMENT_UID,
        HDR_ID,
        RUN_TIMESTAMP,
        VERSION,
        BILLER_CD,
        CREATE_DT,
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
        TELPAY_SOURCE,
        CURRENT_DATE('America/Toronto') AS BI_QUERY_TELPAY_DATE,
        '{run_id}' AS BI_QUERY_DAG_RUN_ID,
        SAFE_CAST(ABS(FARM_FINGERPRINT(GENERATE_UUID())) AS STRING) AS BI_QUERY_TELPAY_UID
    FROM
        (
            SELECT
                DISTINCT CI.CUSTOMER_IDENTIFIER_NO AS PCF_CUST_ID,
                A.MAST_ACCOUNT_ID AS ACCOUNT_NO,
                COALESCE(TLPY.BILLER_ACCT_NUM, '-1') AS CARD_NUMBER,
                TLPY.CUST_NM AS PAYER_NAME,
                TLPY.PYMT_AMT AS AMOUNT,
                TLPY.PYMT_DT AS ORIG_FI_DT,
                TLPY.PYMT_DT AS EFF_PYMT_DT,
                COALESCE(SAFE_CAST(TLPY.ITEM_NUM AS STRING), '-1') AS TRACE_ID,
                SAFE_CAST(TLPY.RPT_NUM AS STRING) AS REPORT_NUMBER,
                NULL AS RUN_NUMBER,
                NULL AS TELPAY_PYMT_UID,
                NULL AS CARD_DEPOSIT_FULFILLMENT_UID,
                COALESCE(SAFE_CAST(TLPY.HDR_ID AS STRING), '-1') AS HDR_ID,
                TELPAY_PYMT_HDR.RUN_TM AS RUN_TIMESTAMP,
                TELPAY_PYMT_HDR.VERSION AS VERSION,
                TELPAY_PYMT_HDR.BILLER_CD AS BILLER_CD,
                TLPY.REC_CREATE_TMS AS CREATE_DT,
                TLPY.REC_CHNG_ID AS CREATE_USER_ID,
                SAFE_CAST(NULL AS STRING) AS CREATE_FUNCTION_NAME,
                COALESCE(TLPY.REC_CHNG_TMS, '2007-01-01') AS UPDATE_DT,
                SAFE_CAST(NULL AS STRING) AS UPDATE_USER_ID,
                SAFE_CAST(NULL AS STRING) AS UPDATE_FUNCTION_NAME,
                '053 : TELPAY_PYMT_TRANS' AS TELPAY_SOURCE,
                ROW_NUMBER() OVER (
                    PARTITION BY TLPY.BILLER_ACCT_NUM,
                    TLPY.ITEM_NUM,
                    TLPY.HDR_ID
                    ORDER BY
                        TLPY.REC_CHNG_TMS DESC
                ) AS REC_RANK
            FROM
                `pcb-{env}-curated.domain_payments.TELPAY_PYMT_TRANS` TLPY
                LEFT JOIN `pcb-{env}-curated.domain_payments.TELPAY_PYMT_HDR` TELPAY_PYMT_HDR ON TLPY.RPT_NUM = TELPAY_PYMT_HDR.RPT_NUM
                AND TLPY.HDR_ID = TELPAY_PYMT_HDR.HDR_ID
                LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCESS_MEDIUM` AM ON AM.CARD_NUMBER = TLPY.BILLER_ACCT_NUM
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