CREATE
OR REPLACE TABLE `{staging_table_id}` OPTIONS (
    expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
) AS (
    SELECT
        DISTINCT EDI.PCF_CUST_ID,
        EDI.ACCOUNT_NO,
        EDI.CARD_NUMBER,
        EDI.PAYER_NAME,
        EDI.AMOUNT,
        EDI.ORIG_FI_DT,
        EDI.EFF_PYMT_DT,
        EDI.TRACE_ID,
        EDI.FI_TRACE_NUM,
        EDI.PAYMENT_TRACE_NO,
        EDI.INSTITUTION_ID,
        EDI.CREATE_DT,
        EDI.HDR_ID,
        EDI.EDI_PYMT_UID,
        EDI.CARD_DEPOSIT_FULFILLMENT_UID,
        EDI.ORIG_CO_SUPP_CD,
        EDI.CREATE_USER_ID,
        EDI.CREATE_FUNCTION_NAME,
        EDI.UPDATE_DT,
        EXTRACT(
            MONTH
            FROM
                EDI.UPDATE_DT
        ) AS UPDATE_MONTH,
        EXTRACT(
            YEAR
            FROM
                EDI.UPDATE_DT
        ) AS UPDATE_YEAR,
        EDI.UPDATE_USER_ID,
        EDI.UPDATE_FUNCTION_NAME,
        EDI.EDI_SOURCE,
        CURRENT_DATE('America/Toronto') AS BI_QUERY_EDI_DATE,
        '{run_id}' AS BI_QUERY_DAG_RUN_ID,
        SAFE_CAST(ABS(FARM_FINGERPRINT(GENERATE_UUID())) AS STRING) AS BI_QUERY_EDI_UID
    FROM
        (
            SELECT
                DISTINCT CI.CUSTOMER_IDENTIFIER_NO AS PCF_CUST_ID,
                A.MAST_ACCOUNT_ID AS ACCOUNT_NO,
                CDF.ACCESS_MEDIUM_NO AS CARD_NUMBER,
                EDI.CUST_NM AS PAYER_NAME,
                EDI.AMOUNT AS AMOUNT,
                EDI.ORIG_FI_DT AS ORIG_FI_DT,
                EDI.EFF_PYMT_DT AS EFF_PYMT_DT,
                EDI.TRACE_ID AS TRACE_ID,
                EDI.FI_TRACE_NUM AS FI_TRACE_NUM,
                EDI.PAYMENT_TRACE_NO AS PAYMENT_TRACE_NO,
                EDI.INSTITUTION_ID AS INSTITUTION_ID,
                EDI.CREATE_DT AS CREATE_DT,
                EDI.HDR_ID AS HDR_ID,
                COALESCE(EDI.EDI_PYMT_INB_UID, -1) AS EDI_PYMT_UID,
                EDI.CARD_DEPOSIT_FULFILLMENT_UID AS CARD_DEPOSIT_FULFILLMENT_UID,
                EDI.TRANSACTION_SET_CONTROL_NO AS ORIG_CO_SUPP_CD,
                EDI.CREATE_USER_ID AS CREATE_USER_ID,
                EDI.CREATE_FUNCTION_NAME AS CREATE_FUNCTION_NAME,
                COALESCE(EDI.UPDATE_DT, '2007-01-01') AS UPDATE_DT,
                EDI.UPDATE_USER_ID AS UPDATE_USER_ID,
                EDI.UPDATE_FUNCTION_NAME AS UPDATE_FUNCTION_NAME,
                'ODS : EDI_PYMT_INB_REQ' AS EDI_SOURCE,
                ROW_NUMBER() OVER (
                    PARTITION BY EDI.EDI_PYMT_INB_UID
                    ORDER BY
                        EDI.UPDATE_DT DESC
                ) AS REC_RANK
            FROM
                `pcb-{env}-curated.domain_payments.EDI_PYMT_INB_REQ` EDI
                LEFT JOIN `pcb-{env}-curated.domain_payments.CARD_DEPOSIT_FULFILLMENT` CDF ON CDF.CARD_DEPOSIT_FULFILLMENT_UID = EDI.CARD_DEPOSIT_FULFILLMENT_UID
                LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCESS_MEDIUM` AM ON AM.CARD_NUMBER = CDF.ACCESS_MEDIUM_NO
                LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` AC ON AC.ACCOUNT_CUSTOMER_UID = AM.ACCOUNT_CUSTOMER_UID
                LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCOUNT` A ON A.ACCOUNT_UID = AC.ACCOUNT_UID
                LEFT JOIN `pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` CI ON CI.CUSTOMER_UID = AC.CUSTOMER_UID
            WHERE
                CI.TYPE = "PCF-CUSTOMER-ID"
                OR CI.TYPE IS NULL
        ) AS EDI
        LEFT JOIN (
            SELECT
                DISTINCT EDI_PYMT_UID
            FROM
                `pcb-{env}-curated.domain_payments.BI_QUERY_EDI_RECORDS_ODS`
        ) existing_records ON EDI.EDI_PYMT_UID = existing_records.EDI_PYMT_UID
        LEFT JOIN (
            SELECT
                EDI_PYMT_UID,
                PCF_CUST_ID,
                ACCOUNT_NO,
                CARD_NUMBER,
                UPDATE_DT
            FROM
                (
                    SELECT
                        EDI_PYMT_UID,
                        PCF_CUST_ID,
                        ACCOUNT_NO,
                        CARD_NUMBER,
                        UPDATE_DT,
                        ROW_NUMBER() OVER (
                            PARTITION BY EDI_PYMT_UID
                            ORDER BY
                                BI_QUERY_EDI_DATE DESC,
                                BI_QUERY_DAG_RUN_ID DESC
                        ) as rn
                    FROM
                        `pcb-{env}-curated.domain_payments.BI_QUERY_EDI_RECORDS_ODS`
                )
            WHERE
                rn = 1
        ) latest_records ON EDI.EDI_PYMT_UID = latest_records.EDI_PYMT_UID
    WHERE
        EDI.REC_RANK = 1
        AND (
            -- New records (no existing record found)
            existing_records.EDI_PYMT_UID IS NULL -- OR records that need updates (have new data for critical fields)
            OR (
                latest_records.EDI_PYMT_UID IS NOT NULL
                AND (
                    -- Critical fields that were NULL before and now have values
                    (
                        latest_records.PCF_CUST_ID IS NULL
                        AND EDI.PCF_CUST_ID IS NOT NULL
                    )
                    OR (
                        latest_records.ACCOUNT_NO IS NULL
                        AND EDI.ACCOUNT_NO IS NOT NULL
                    )
                    OR (
                        latest_records.CARD_NUMBER IS NULL
                        AND EDI.CARD_NUMBER IS NOT NULL
                    ) -- General update check
                    OR latest_records.UPDATE_DT < EDI.UPDATE_DT
                )
            )
        )
);