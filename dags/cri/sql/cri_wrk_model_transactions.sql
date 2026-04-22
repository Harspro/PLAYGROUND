-- CRI Model BQ Task 8: Process model transactions (Deloitte: 2. Working Tables/5. process model transactions.sql)
-- Table created via schema: cri_wrk_model_transactions.sql
-- Append-only: each run inserts a new batch; reads current run from CRI_WRK_TRANSACTIONS_LATEST and CRI_WRK_ACCOUNT_MASTER_LATEST via JobId.
-- Parameters: {dag_id}, {env}

INSERT INTO `pcb-{env}-landing.domain_cri_compliance.CRI_WRK_MODEL_TRANSACTIONS`
  (
    TransactionId,
    AccountId,
    ModelTradeDate,
    DebitCreditIndicator,
    FeeOnlyFlag,
    InScopeFeeIntFlag,
    TransactionDescription,
    TransactionAmount,
    TransactionAmountForFv,
    TransactionAmountForFvOffset,
    RecLoadTimestamp,
    JobId)
WITH
  wrk AS (
    SELECT
      t.TransactionId,
      t.AccountId,
      SAFE.PARSE_DATE('%Y-%m-%d', t.ModelTradeDate) AS ModelTradeDate,
      t.DebitCreditIndicator,
      t.FeeOnlyFlag,
      t.InScopeFeeIntFlag,
      t.TransactionDescription,
      SAFE_CAST(t.TransactionAmount AS FLOAT64) AS TransactionAmount,
      SAFE_CAST(b.BalanceAtStart AS FLOAT64) AS BalanceAtStart
    FROM `pcb-{env}-curated.domain_cri_compliance.CRI_WRK_TRANSACTIONS_LATEST` t
    INNER JOIN
      `pcb-{env}-curated.domain_cri_compliance.CRI_WRK_ACCOUNT_MASTER_LATEST` b
      ON t.AccountId = b.AccountId
  ),
  signed_txn AS (
    SELECT
      TransactionId,
      AccountId,
      ModelTradeDate,
      DebitCreditIndicator,
      FeeOnlyFlag,
      InScopeFeeIntFlag,
      TransactionDescription,
      CASE
        WHEN UPPER(TRIM(DebitCreditIndicator)) = 'C'
          THEN -1.0 * TransactionAmount
        ELSE 1.0 * TransactionAmount
        END AS TransactionAmount,
      BalanceAtStart
    FROM wrk
  ),
  with_balance AS (
    SELECT
      *,
      TransactionAmount AS TransactionAmountForFv,
      CAST(0 AS FLOAT64) AS TransactionAmountForFvOffset,
      CAST('' AS STRING) AS FlagStoredCredit,
      SUM(TransactionAmount)
        OVER (
          PARTITION BY AccountId
          ORDER BY
            ModelTradeDate,
            CASE DebitCreditIndicator WHEN 'D' THEN 0 ELSE 1 END,
            InScopeFeeIntFlag, TransactionAmount DESC, TransactionId
        )
        + BalanceAtStart AS CurrentProxyBalance,
      SUM(TransactionAmount)
        OVER (
          PARTITION BY AccountId
          ORDER BY
            ModelTradeDate,
            CASE DebitCreditIndicator WHEN 'D' THEN 0 ELSE 1 END,
            InScopeFeeIntFlag, TransactionAmount DESC, TransactionId
        )
        + BalanceAtStart
        - TransactionAmount AS PrevProxyBalance
    FROM signed_txn
  ),
  after_credit AS (
    SELECT
      *
        REPLACE (
          CASE
            WHEN
              UPPER(TRIM(DebitCreditIndicator)) = 'C'
              AND CurrentProxyBalance < 0
              THEN IF(PrevProxyBalance < 0, 0, -PrevProxyBalance)
            ELSE TransactionAmountForFv
            END AS TransactionAmountForFv,
          CASE
            WHEN
              UPPER(TRIM(DebitCreditIndicator)) = 'C'
              AND CurrentProxyBalance < 0
              THEN 'Add'
            ELSE FlagStoredCredit
            END AS FlagStoredCredit)
    FROM with_balance
  ),
  after_debit AS (
    SELECT
      *
        REPLACE (
          CASE
            WHEN
              UPPER(TRIM(DebitCreditIndicator)) = 'D'
              AND PrevProxyBalance < 0
              THEN
                CASE
                  WHEN CurrentProxyBalance < 0 THEN 0
                  ELSE CurrentProxyBalance
                  END
            ELSE TransactionAmountForFv
            END AS TransactionAmountForFv,
          CASE
            WHEN
              UPPER(TRIM(DebitCreditIndicator)) = 'D'
              AND PrevProxyBalance < 0
              THEN
                TransactionAmount - CASE
                  WHEN CurrentProxyBalance < 0 THEN 0
                  ELSE CurrentProxyBalance
                  END
            ELSE TransactionAmountForFvOffset
            END AS TransactionAmountForFvOffset,
          CASE
            WHEN
              UPPER(TRIM(DebitCreditIndicator)) = 'D'
              AND PrevProxyBalance < 0
              THEN 'Use'
            ELSE FlagStoredCredit
            END AS FlagStoredCredit)
    FROM after_credit
  )
SELECT
  TransactionId,
  AccountId,
  FORMAT_DATE('%Y-%m-%d', ModelTradeDate) AS ModelTradeDate,
  DebitCreditIndicator,
  FeeOnlyFlag,
  InScopeFeeIntFlag,
  TransactionDescription,
  CAST(ABS(TransactionAmount) AS STRING) AS TransactionAmount,
  CAST(ABS(TransactionAmountForFv) AS STRING) AS TransactionAmountForFv,
  CAST(ABS(TransactionAmountForFvOffset) AS STRING)
    AS TransactionAmountForFvOffset,
  CURRENT_DATETIME('America/Toronto') AS RecLoadTimestamp,
  '{dag_id}' AS JobId
FROM after_debit;
