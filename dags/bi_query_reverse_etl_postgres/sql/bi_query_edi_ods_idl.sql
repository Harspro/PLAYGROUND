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
        )
    WHERE
        REC_RANK = 1
);