BEGIN

  -- Declare date and threshold
  DECLARE rundate DATE DEFAULT CURRENT_DATE();

DECLARE threshold DATE;

-- Set start date for cifp_first_use_date
SET threshold = date_sub(rundate, INTERVAL 180 day);

INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_TRANS_HIST_BASE`
(
  FILE_CREATE_DT,
  MAST_ACCOUNT_ID,
  AT31_ACCOUNT_NUMB,
  AT31_DATE_POST,
  AT31_DATE_TRANSACTION,
  AT31_DEBIT_CREDIT_INDICATOR,
  AT31_TERMS_BALANCE_CODE,
  TRANS_AMT,
  MCC,
  MERCHANT_ID,
  MERCHANT_NAME,
  TRANS_CODE,
  TRANS_CATEGORY,
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
SELECT DISTINCT
  CAST(file_create_dt AS STRING) AS FILE_CREATE_DT,
  CAST(mast_account_id AS STRING) AS MAST_ACCOUNT_ID,
  at31_account_numb AS AT31_ACCOUNT_NUMB,
  CAST(at31_date_post AS STRING) AS AT31_DATE_POST,
  CAST(at31_date_transaction AS STRING) AS AT31_DATE_TRANSACTION,
  at31_debit_credit_indicator AS AT31_DEBIT_CREDIT_INDICATOR,
  CAST(at31_terms_balance_code AS STRING) AS AT31_TERMS_BALANCE_CODE,
  CAST(
    CASE
      WHEN at31_debit_credit_indicator IN ('C') THEN (-1) * at31_amt_transaction
      ELSE at31_amt_transaction
      END AS STRING)
    AS TRANS_AMT,
  CAST(at31_merchant_category_code AS STRING) AS MCC,
  at31_merchant_id AS MERCHANT_ID,
  at31_merchant_dba_name AS MERCHANT_NAME,
  CAST(at31_transaction_code AS STRING) AS TRANS_CODE,
  CAST(at31_transaction_category AS STRING) AS TRANS_CATEGORY,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_account_management.AT31`
WHERE
  -- Added Partition column, FILE_CREATE_DATE in filter condition with same 6 month threshold.
  FILE_CREATE_DT >= threshold
  AND FILE_CREATE_DT < rundate
  AND at31_date_post >= threshold
  AND at31_date_post < rundate
  AND at31_terms_balance_code NOT IN (0, 3)
  AND CAST(mast_account_id AS STRING)
    IN (
      SELECT
        DISTINCT mast_account_id
      FROM
        `pcb-{env}-curated.domain_scoring.SCORING_PREP_SCORE_POP_DAILY_LATEST`
    );

END
