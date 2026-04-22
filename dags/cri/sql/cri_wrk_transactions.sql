-- CRI Model BQ Task 7: Populate wrk transactions (Deloitte: 2. Working Tables/4. populate wrk transactions.sql)
-- Append-only: each run inserts a new batch; reads current run from stg/wrk tables via JOB_ID.
-- Parameters: {dag_id}, {env}

INSERT INTO `pcb-{env}-landing.domain_cri_compliance.CRI_WRK_TRANSACTIONS`
  (
    TransactionId,
    AccountId,
    PostDate,
    TransactionDate,
    ModelTradeDate,
    TransactionDescription,
    DebitCreditIndicator,
    TransactionCategory,
    TransactionCode,
    TransactionAmount,
    InScopeFeeIntFlag,
    FeeOnlyFlag,
    RecLoadTimestamp,
    JobId)
WITH
  -- Pre-process transactions with parsed dates and type casting
  transactions AS (
    SELECT
      TransactionId,
      AccountId,
      SAFE.PARSE_DATE('%Y-%m-%d', PostDate) AS PostDate,
      SAFE.PARSE_DATE('%Y-%m-%d', TransactionDate) AS TransactionDate,
      TransactionDescription,
      DebitCreditIndicator,
      TransactionCategory,
      TransactionCode,
      TransactionAmount
    FROM `pcb-{env}-curated.domain_cri_compliance.CRI_STG_TRANSACTIONS_LATEST`
  ),
  -- Pre-process account master with parsed dates
  account_master AS (
    SELECT
      AccountId,
      SAFE.PARSE_DATE('%Y-%m-%d', FirstDayOfEvaluation) AS FirstDayOfEvaluation,
      SAFE.PARSE_DATE('%Y-%m-%d', LastDayOfEvaluation) AS LastDayOfEvaluation
    FROM `pcb-{env}-curated.domain_cri_compliance.CRI_WRK_ACCOUNT_MASTER_LATEST`
  ),
  -- Pre-process lookup table with parsed dates and type casting
  lookup_fees_interest AS (
    SELECT DISTINCT
      TCat,
      TCode,
      Classification,
      Active,
      SAFE.PARSE_DATE('%Y-%m-%d', EffectiveStartDate) AS EffectiveStartDate,
      SAFE.PARSE_DATE('%Y-%m-%d', EffectiveEndDate) AS EffectiveEndDate
    FROM
      `pcb-{env}-curated.domain_cri_compliance.CRI_WRK_LOOKUP_TCAT_TCODE_CLASSIFICATION_LATEST`
  ),
  -- Join transactions with account master and lookup
  joined_data AS (
    SELECT
      t.TransactionId,
      t.AccountId,
      t.PostDate,
      t.TransactionDate,
      -- Calculate ModelTradeDate once
      IF(
        DATE_DIFF(t.PostDate, t.TransactionDate, DAY) > 7,
        t.PostDate,
        t.TransactionDate) AS ModelTradeDate,
      t.TransactionDescription,
      t.DebitCreditIndicator,
      t.TransactionCategory,
      t.TransactionCode,
      t.TransactionAmount,
      -- Pre-calculate flags
      CAST(IF(UPPER(c.Classification) IN ('FEE', 'INTEREST'), 1, 0) AS STRING)
        AS InScopeFeeIntFlag,
      CAST(IF(UPPER(c.Classification) = 'FEE', 1, 0) AS STRING) AS FeeOnlyFlag,
    FROM transactions t
    INNER JOIN account_master a
      ON
        t.AccountId = a.AccountId
        AND t.PostDate BETWEEN a.FirstDayOfEvaluation AND a.LastDayOfEvaluation
    LEFT JOIN lookup_fees_interest c
      ON
        t.TransactionCategory = c.Tcat
        AND t.TransactionCode = c.Tcode
        AND t.TransactionDate
          BETWEEN c.EffectiveStartDate
          AND c.EffectiveEndDate
    WHERE
      -- Case 1: no lookup row at all for this (Tcat, Tcode, date) → keep, unclassified
      (c.Tcat IS NULL AND c.Tcode IS NULL)
      OR

        -- Case 2: lookup row exists AND is active AND not explicitly excluded → keep
        (
          UPPER(c.Active) = 'Y'
          AND (c.Classification IS NULL OR UPPER(c.Classification) != 'EXCLUDE'))
  )
SELECT
  TransactionId AS TransactionId,
  AccountId AS AccountId,
  FORMAT_DATE('%Y-%m-%d', PostDate) AS PostDate,
  FORMAT_DATE('%Y-%m-%d', TransactionDate) AS TransactionDate,
  FORMAT_DATE('%Y-%m-%d', ModelTradeDate) AS ModelTradeDate,
  TransactionDescription,
  DebitCreditIndicator,
  CAST(TransactionCategory AS STRING) AS TransactionCategory,
  CAST(TransactionCode AS STRING) AS TransactionCode,
  TransactionAmount,
  InScopeFeeIntFlag,
  FeeOnlyFlag,
  CURRENT_DATETIME('America/Toronto') AS RecLoadTimestamp,
  '{dag_id}' AS JobId
FROM joined_data;
