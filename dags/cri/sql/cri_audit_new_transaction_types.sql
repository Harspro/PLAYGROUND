-- CRI Model BQ Task 10: Detect new transaction types (Deloitte: 3. Model Logic/1. run model.sql - Section 8).
-- Append-only: each run inserts newly detected TCAT/TCODE combinations; reads current run from WRK_TRANSACTIONS via JobId.
-- Parameters: {dag_id}, {env}

INSERT INTO `pcb-{env}-landing.domain_cri_compliance.CRI_AUDIT_NEW_TRANSACTION_TYPES`
  (
    Tcat,
    Tcode,
    TransactionCount,
    EarliestTransactionDate,
    LatestTransactionDate,
    RecLoadTimestamp,
    JobId)
WITH
  wrk_transactions AS (
    SELECT
      vw_cri_wrk_txn.TransactionCategory,
      vw_cri_wrk_txn.TransactionCode,
      vw_cri_wrk_txn.TransactionDate
    FROM
      `pcb-{env}-curated.domain_cri_compliance.CRI_WRK_TRANSACTIONS_LATEST` AS vw_cri_wrk_txn
  ),
  lookup_master AS (
    SELECT DISTINCT
      vw_cri_wrk_lkp_tcat_tcode.Tcat,
      vw_cri_wrk_lkp_tcat_tcode.Tcode
    FROM
      `pcb-{env}-curated.domain_cri_compliance.CRI_WRK_LOOKUP_TCAT_TCODE_CLASSIFICATION_ALL` AS vw_cri_wrk_lkp_tcat_tcode
  ),
  new_types AS (
    SELECT
      cte_wrk_txn.TransactionCategory,
      cte_wrk_txn.TransactionCode,
      COUNT(*) AS TransactionCount,
      MIN(cte_wrk_txn.TransactionDate) AS EarliestTransactionDate,
      MAX(cte_wrk_txn.TransactionDate) AS LatestTransactionDate
    FROM wrk_transactions AS cte_wrk_txn
    LEFT JOIN lookup_master AS cte_lkp_master
      ON
        cte_wrk_txn.TransactionCategory = cte_lkp_master.Tcat
        AND cte_wrk_txn.TransactionCode = cte_lkp_master.Tcode
    WHERE cte_lkp_master.Tcat IS NULL AND cte_lkp_master.Tcode IS NULL
    GROUP BY cte_wrk_txn.TransactionCategory, cte_wrk_txn.TransactionCode
  )
SELECT
  CAST(cte_new_types.TransactionCategory AS STRING) AS Tcat,
  CAST(cte_new_types.TransactionCode AS STRING) AS Tcode,
  CAST(cte_new_types.TransactionCount AS STRING),
  cte_new_types.EarliestTransactionDate,
  cte_new_types.LatestTransactionDate,
  CURRENT_DATETIME('America/Toronto') AS RecLoadTimestamp,
  '{dag_id}' AS JobId
FROM new_types AS cte_new_types;
