-- Inserting records from daily run
INSERT INTO
    `pcb-{env}-landing.domain_payments.BI_QUERY_EDI_RECORDS_053` (
        PCF_CUST_ID,
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
        UPDATE_MONTH,
        UPDATE_YEAR,
        UPDATE_USER_ID,
        UPDATE_FUNCTION_NAME,
        EDI_SOURCE,
        BI_QUERY_EDI_DATE,
        BI_QUERY_DAG_RUN_ID,
        BI_QUERY_EDI_UID
    )
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
    UPDATE_MONTH,
    UPDATE_YEAR,
    UPDATE_USER_ID,
    UPDATE_FUNCTION_NAME,
    EDI_SOURCE,
    BI_QUERY_EDI_DATE,
    BI_QUERY_DAG_RUN_ID,
    BI_QUERY_EDI_UID
FROM
    `pcb-{env}-processing.domain_payments.BI_QUERY_EDI_053` EDI
WHERE
    NOT EXISTS(
        SELECT
            1
        FROM
            `pcb-{env}-curated.domain_payments.BI_QUERY_EDI_RECORDS_053` EDI_REC
        WHERE
            EDI.CARD_NUMBER = EDI_REC.CARD_NUMBER
            AND EDI.TRACE_ID = EDI_REC.TRACE_ID
            AND EDI.HDR_ID = EDI_REC.HDR_ID
            AND EDI.BI_QUERY_DAG_RUN_ID = EDI_REC.BI_QUERY_DAG_RUN_ID
    );