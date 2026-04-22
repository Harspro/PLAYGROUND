-- CRI Model BQ Task 3: Data quality validation (Deloitte: 3. Model Logic/1. run model.sql - Section 2)
-- Append-only: each run inserts data quality issues detected; reads current run from STG tables via JobId.
-- Parameters: {dag_id}, {env}

INSERT INTO `pcb-{env}-landing.domain_cri_compliance.CRI_AUDIT_DATA_ISSUES`
  (EntityType, EntityId, ProductType, IssueDescription, RecLoadTimestamp, JobId)
WITH
  stg_accounts AS (
    SELECT
      AccountId,
      ProductType,
      FirstDayOfEvaluation,
      LastDayOfEvaluation,
      BalanceAtStart,
      OpenDate
    FROM `pcb-{env}-curated.domain_cri_compliance.CRI_STG_ACCOUNT_MASTER_LATEST`
  ),
  stg_transactions AS (
    SELECT
      TransactionId,
      AccountId,
      PostDate,
      TransactionDate,
      TransactionDescription,
      DebitCreditIndicator,
      TransactionCategory,
      TransactionCode,
      TransactionAmount
    FROM `pcb-{env}-curated.domain_cri_compliance.CRI_STG_TRANSACTIONS_LATEST`
  ),
  issues AS (
    -- Duplicate accounts
    SELECT
      'ACCOUNT' AS EntityType,
      AccountId AS EntityId,
      ProductType,
      'Account has duplicate records' AS IssueDescription
    FROM stg_accounts
    GROUP BY AccountId, ProductType
    HAVING COUNT(1) > 1
    UNION ALL

    -- Accounts with missing data
    SELECT
      'ACCOUNT',
      COALESCE(AccountId, 'NULL'),
      ProductType,
      'Account has missing data'
    FROM stg_accounts
    WHERE
      AccountId IS NULL
      OR ProductType IS NULL
      OR FirstDayOfEvaluation IS NULL
      OR LastDayOfEvaluation IS NULL
      OR BalanceAtStart IS NULL
      OR OpenDate IS NULL
    UNION ALL

    -- Duplicate transactions
    SELECT
      'TRANSACTION',
      TransactionId,
      CAST(NULL AS STRING),
      'Transaction has duplicate records'
    FROM stg_transactions
    GROUP BY TransactionId
    HAVING COUNT(1) > 1
    UNION ALL

    -- Transactions with missing data
    SELECT
      'TRANSACTION',
      COALESCE(TransactionId, 'NULL'),
      CAST(NULL AS STRING),
      'Transaction has missing data'
    FROM stg_transactions
    WHERE
      AccountId IS NULL
      OR PostDate IS NULL
      OR TransactionDate IS NULL
      OR TransactionDescription IS NULL
      OR DebitCreditIndicator IS NULL
      OR TransactionCategory IS NULL
      OR TransactionCode IS NULL
      OR TransactionAmount IS NULL
  )
SELECT
  EntityType,
  EntityId,
  ProductType,
  IssueDescription,
  CURRENT_DATETIME('America/Toronto') AS RecLoadTimestamp,
  '{dag_id}' AS JobId
FROM issues;
