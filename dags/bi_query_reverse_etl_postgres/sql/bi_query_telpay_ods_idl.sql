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
        )
    WHERE
        REC_RANK = 1
);