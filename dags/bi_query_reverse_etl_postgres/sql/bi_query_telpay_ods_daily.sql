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
                CDF.ACCESS_MEDIUM_NO AS CARD_NUMBER,
                TELPAY.CUST_NM AS PAYER_NAME,
                TELPAY.AMOUNT AS AMOUNT,
                TELPAY.ORIG_FI_DT AS ORIG_FI_DT,
                TELPAY.EFF_PYMT_DT AS EFF_PYMT_DT,
                TELPAY.TRACE_ID AS TRACE_ID,
                TELPAY.RPT_NUM AS REPORT_NUMBER,
                TELPAY.RUN_NUM AS RUN_NUMBER,
                COALESCE(TELPAY.TELPAY_PYMT_INB_UID, -1) AS TELPAY_PYMT_UID,
                TELPAY.CARD_DEPOSIT_FULFILLMENT_UID AS CARD_DEPOSIT_FULFILLMENT_UID,
                TELPAY.HDR_ID AS HDR_ID,
                TELPAY.RUN_TMSTMP AS RUN_TIMESTAMP,
                TELPAY.VERSION AS VERSION,
                TELPAY.BILLER_CD AS BILLER_CD,
                TELPAY.CREATE_DT AS CREATE_DT,
                TELPAY.CREATE_USER_ID AS CREATE_USER_ID,
                TELPAY.CREATE_FUNCTION_NAME AS CREATE_FUNCTION_NAME,
                COALESCE(TELPAY.UPDATE_DT, '2007-01-01') AS UPDATE_DT,
                TELPAY.UPDATE_USER_ID AS UPDATE_USER_ID,
                TELPAY.UPDATE_FUNCTION_NAME AS UPDATE_FUNCTION_NAME,
                'ODS : TELPAY_PYMT_INB_REQ' AS TELPAY_SOURCE,
                ROW_NUMBER() OVER (
                    PARTITION BY TELPAY.TELPAY_PYMT_INB_UID
                    ORDER BY
                        TELPAY.UPDATE_DT DESC
                ) AS REC_RANK
            FROM
                `pcb-{env}-curated.domain_payments.TELPAY_PYMT_INB_REQ` TELPAY
                LEFT JOIN `pcb-{env}-curated.domain_payments.CARD_DEPOSIT_FULFILLMENT` CDF ON CDF.CARD_DEPOSIT_FULFILLMENT_UID = TELPAY.CARD_DEPOSIT_FULFILLMENT_UID
                LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCESS_MEDIUM` AM ON AM.CARD_NUMBER = CDF.ACCESS_MEDIUM_NO
                LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` AC ON AC.ACCOUNT_CUSTOMER_UID = AM.ACCOUNT_CUSTOMER_UID
                LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCOUNT` A ON A.ACCOUNT_UID = AC.ACCOUNT_UID
                LEFT JOIN `pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` CI ON CI.CUSTOMER_UID = AC.CUSTOMER_UID
            WHERE
                CI.TYPE = "PCF-CUSTOMER-ID"
                OR CI.TYPE IS NULL
        ) AS TELPAY
        LEFT JOIN (
            SELECT
                DISTINCT TELPAY_PYMT_UID
            FROM
                `pcb-{env}-curated.domain_payments.BI_QUERY_TELPAY_RECORDS_ODS`
        ) existing_records ON TELPAY.TELPAY_PYMT_UID = existing_records.TELPAY_PYMT_UID
        LEFT JOIN (
            SELECT
                TELPAY_PYMT_UID,
                PCF_CUST_ID,
                ACCOUNT_NO,
                CARD_NUMBER,
                UPDATE_DT
            FROM
                (
                    SELECT
                        TELPAY_PYMT_UID,
                        PCF_CUST_ID,
                        ACCOUNT_NO,
                        CARD_NUMBER,
                        UPDATE_DT,
                        ROW_NUMBER() OVER (
                            PARTITION BY TELPAY_PYMT_UID
                            ORDER BY
                                BI_QUERY_TELPAY_DATE DESC,
                                BI_QUERY_DAG_RUN_ID DESC
                        ) as rn
                    FROM
                        `pcb-{env}-curated.domain_payments.BI_QUERY_TELPAY_RECORDS_ODS`
                )
            WHERE
                rn = 1
        ) latest_records ON TELPAY.TELPAY_PYMT_UID = latest_records.TELPAY_PYMT_UID
    WHERE
        TELPAY.REC_RANK = 1
        AND (
            -- New records (no existing record found)
            existing_records.TELPAY_PYMT_UID IS NULL -- OR records that need updates (have new data for critical fields)
            OR (
                latest_records.TELPAY_PYMT_UID IS NOT NULL
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