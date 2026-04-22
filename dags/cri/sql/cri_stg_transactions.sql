-- CRI Model BQ Task 2: Stage transaction data (Deloitte: 1. Staging Tables/2. stage transaction data.sql)
-- Table created via schema: jira_ticket_36_cri_stg_transactions_table.md
-- Append-only: each run inserts a new batch; reads current run from CRI_STG_ACCOUNT_MASTER via JobId.
--
-- Sources:
--   - pcb-{env}-curated.domain_account_management.AT31 (schema: requirement_details/at31.json)
--   - ACCOUNT_CRI_EVALUATION (bridge: AT31.MAST_ACCOUNT_ID = LedgerAccountId; output AccountId = InternalAccountId)
--   - CRI_STG_ACCOUNT_MASTER (filter JobId, date range)
-- AT31 columns used: MAST_ACCOUNT_ID (NUMERIC), AT31_DATE_POST, AT31_DATE_TRANSACTION, AT31_TRANSACTION_DESCRIPTION,
--   AT31_DEBIT_CREDIT_INDICATOR, AT31_TRANSACTION_CATEGORY, AT31_TRANSACTION_CODE (NUMERIC), AT31_AMT_TRANSACTION (NUMERIC).
--
-- Parameters: {cri_evaluation_date}, {job_id}, dev

INSERT INTO `pcb-{env}-landing.domain_cri_compliance.CRI_STG_TRANSACTIONS` (
  TransactionId, AccountId, PostDate, TransactionDate, TransactionDescription, DebitCreditIndicator, TransactionCategory, TransactionCode, TransactionAmount, RecLoadTimestamp, JobId
)
WITH
  -- Pre-calculate date range once to avoid redundant subqueries
  date_range AS (
    SELECT
      MIN(SAFE.PARSE_DATE('%Y-%m-%d', FirstDayOfEvaluation)) AS min_eval_date,
      MAX(SAFE.PARSE_DATE('%Y-%m-%d', LastDayOfEvaluation)) AS max_eval_date
    FROM `pcb-{env}-curated.domain_cri_compliance.CRI_STG_ACCOUNT_MASTER_LATEST`
  ),
  -- Pre-process account master with parsed dates to avoid repeated parsing
  account_master AS (
    SELECT
      AccountId,
      SAFE.PARSE_DATE('%Y-%m-%d', FirstDayOfEvaluation) AS FirstDayOfEvaluation,
      SAFE.PARSE_DATE('%Y-%m-%d', LastDayOfEvaluation) AS LastDayOfEvaluation
    FROM `pcb-{env}-curated.domain_cri_compliance.CRI_STG_ACCOUNT_MASTER_LATEST`
  ),
  -- Pre-process transactions with date calculations
  transactions AS (
    SELECT
      t.MAST_ACCOUNT_ID,
      DATE(t.AT31_DATE_POST) AS post_date,
      DATE(t.AT31_DATE_TRANSACTION) AS transaction_date,
      t.AT31_REFERENCE_NUM,
      t.AT31_TRANSACTION_DESCRIPTION,
      t.AT31_DEBIT_CREDIT_INDICATOR,
      t.AT31_TRANSACTION_CATEGORY,
      t.AT31_TRANSACTION_CODE,
      t.AT31_AMT_TRANSACTION
    FROM `pcb-{env}-curated.domain_account_management.AT31` t
    CROSS JOIN date_range dr
    WHERE DATE(t.AT31_DATE_POST) >= dr.min_eval_date
      AND DATE(t.AT31_DATE_POST) <= dr.max_eval_date
  ),
  -- Join transactions with account evaluation and account master
  joined_data AS (
    SELECT
      t.MAST_ACCOUNT_ID,
      t.post_date,
      t.transaction_date,
      t.AT31_REFERENCE_NUM,
      e.InternalAccountId,
      t.AT31_TRANSACTION_DESCRIPTION,
      t.AT31_DEBIT_CREDIT_INDICATOR,
      t.AT31_TRANSACTION_CATEGORY,
      t.AT31_TRANSACTION_CODE,
      t.AT31_AMT_TRANSACTION
    FROM transactions t
    INNER JOIN `pcb-{env}-curated.domain_cri_compliance.ACCOUNT_CRI_EVALUATION_LATEST` e
      ON CAST(t.MAST_ACCOUNT_ID AS STRING) = e.LedgerAccountId
      AND e.CriEvaluationDate = '{cri_evaluation_date}'
    INNER JOIN account_master a
      ON e.InternalAccountId = a.AccountId
        AND t.post_date BETWEEN a.FirstDayOfEvaluation AND a.LastDayOfEvaluation
  )
SELECT
  CAST(ROW_NUMBER() OVER (ORDER BY j.MAST_ACCOUNT_ID, j.post_date, j.AT31_REFERENCE_NUM) AS STRING) AS TransactionId,
  j.InternalAccountId AS AccountId,
  FORMAT_DATE('%Y-%m-%d', j.post_date) AS PostDate,
  FORMAT_DATE('%Y-%m-%d', j.transaction_date) AS TransactionDate,
  j.AT31_TRANSACTION_DESCRIPTION AS TransactionDescription,
  j.AT31_DEBIT_CREDIT_INDICATOR AS DebitCreditIndicator,
  CAST(SAFE_CAST(j.AT31_TRANSACTION_CATEGORY AS INT64) AS STRING) AS TransactionCategory,
  CAST(SAFE_CAST(j.AT31_TRANSACTION_CODE AS INT64) AS STRING) AS TransactionCode,
  CAST(SAFE_CAST(j.AT31_AMT_TRANSACTION AS FLOAT64) AS STRING) AS TransactionAmount,
  CURRENT_DATETIME('America/Toronto') AS RecLoadTimestamp,
  '{job_id}' AS JobId
FROM joined_data j;
