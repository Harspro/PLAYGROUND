CREATE
OR REPLACE TABLE `{staging_table_id}` OPTIONS (
    expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
) AS (
    SELECT
        DISTINCT TELPAY.PCF_CUST_ID,
        TELPAY.ACCOUNT_NO,
        TELPAY.CARD_NUMBER,
        TELPAY.PAYER_NAME,
        TELPAY.AMOUNT,
        TELPAY.ORIG_FI_DT,
        TELPAY.EFF_PYMT_DT,
        TELPAY.TRACE_ID,
        TELPAY.REPORT_NUMBER,
        SAFE_CAST(TELPAY.RUN_NUMBER AS STRING) AS RUN_NUMBER,
        TELPAY.TELPAY_PYMT_UID,
        TELPAY.CARD_DEPOSIT_FULFILLMENT_UID,
        TELPAY.HDR_ID,
        TELPAY.RUN_TIMESTAMP,
        TELPAY.VERSION,
        TELPAY.BILLER_CD,
        TELPAY.CREATE_DT,
        TELPAY.CREATE_USER_ID,
        TELPAY.CREATE_FUNCTION_NAME,
        TELPAY.UPDATE_DT,
        EXTRACT(
            MONTH
            FROM
                TELPAY.UPDATE_DT
        ) AS UPDATE_MONTH,
        EXTRACT(
            YEAR
            FROM
                TELPAY.UPDATE_DT
        ) AS UPDATE_YEAR,
        TELPAY.UPDATE_USER_ID,
        TELPAY.UPDATE_FUNCTION_NAME,
        TELPAY.TELPAY_SOURCE,
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
        ) AS TELPAY
        LEFT JOIN (
            SELECT
                DISTINCT CARD_NUMBER,
                TRACE_ID,
                HDR_ID
            FROM
                `pcb-{env}-curated.domain_payments.BI_QUERY_TELPAY_RECORDS_053`
        ) existing_records ON TELPAY.CARD_NUMBER = existing_records.CARD_NUMBER
        AND TELPAY.TRACE_ID = existing_records.TRACE_ID
        AND TELPAY.HDR_ID = existing_records.HDR_ID
        LEFT JOIN (
            SELECT
                CARD_NUMBER,
                TRACE_ID,
                HDR_ID,
                PCF_CUST_ID,
                ACCOUNT_NO,
                UPDATE_DT
            FROM
                (
                    SELECT
                        CARD_NUMBER,
                        TRACE_ID,
                        HDR_ID,
                        PCF_CUST_ID,
                        ACCOUNT_NO,
                        UPDATE_DT,
                        ROW_NUMBER() OVER (
                            PARTITION BY CARD_NUMBER,
                            TRACE_ID,
                            HDR_ID
                            ORDER BY
                                BI_QUERY_TELPAY_DATE DESC,
                                BI_QUERY_DAG_RUN_ID DESC
                        ) as rn
                    FROM
                        `pcb-{env}-curated.domain_payments.BI_QUERY_TELPAY_RECORDS_053`
                )
            WHERE
                rn = 1
        ) latest_records ON TELPAY.CARD_NUMBER = latest_records.CARD_NUMBER
        AND TELPAY.TRACE_ID = latest_records.TRACE_ID
        AND TELPAY.HDR_ID = latest_records.HDR_ID
    WHERE
        TELPAY.REC_RANK = 1
        AND (
            -- New records (no existing record found)
            existing_records.CARD_NUMBER IS NULL -- OR records that need updates (have new data for critical fields)
            OR (
                latest_records.CARD_NUMBER IS NOT NULL
                AND (
                    -- Critical fields that were NULL before and now have values
                    (
                        latest_records.PCF_CUST_ID IS NULL
                        AND TELPAY.PCF_CUST_ID IS NOT NULL
                    )
                    OR (
                        latest_records.ACCOUNT_NO IS NULL
                        AND TELPAY.ACCOUNT_NO IS NOT NULL
                    )
                    OR (
                        latest_records.CARD_NUMBER IS NULL
                        AND TELPAY.CARD_NUMBER IS NOT NULL
                    ) -- General update check
                    OR latest_records.UPDATE_DT < TELPAY.UPDATE_DT
                )
            )
        )
);