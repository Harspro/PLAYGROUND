CREATE OR REPLACE VIEW `pcb-{env}-processing.cots_alm_tbsm.AT31_AGG_TRANS` AS
SELECT
    COALESCE( 
    SUM(
    CASE
    WHEN AT31_DEBIT_CREDIT_INDICATOR = {at31_debit_credit_indicator_val_1} AND AT31_TERMS_BALANCE_CODE != {at31_terms_balance_code_val} THEN -ABS(AT31_AMT_TRANSACTION)
    WHEN AT31_DEBIT_CREDIT_INDICATOR = {at31_debit_credit_indicator_val_2} AND AT31_TERMS_BALANCE_CODE = {at31_terms_balance_code_val} THEN -ABS(AT31_AMT_TRANSACTION)
    ELSE ABS(AT31_AMT_TRANSACTION)
    END),0) AS AMT_INT,
    MAST_ACCOUNT_ID,
    FILE_CREATE_DT
FROM
    pcb-{env}-processing.cots_alm_tbsm.AT31_DAT
WHERE
    AT31_TRNS_ACCT_FUNC IN ({at31_trns_acct_func_val})
GROUP BY
    MAST_ACCOUNT_ID,
    FILE_CREATE_DT