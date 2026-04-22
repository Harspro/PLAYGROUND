-- Append-only: each run inserts a new batch differentiated by RecLoadTimestamp and JobId.
--
-- Sources:
--   - ACCOUNT_CRI_EVALUATION (filter: CriEvaluationDate = '{cri_evaluation_date}')
--   - pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM00_REC
--   - pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM02_REC
--     (join on MAST_ACCOUNT_ID and AM00_APPLICATION_SUFFIX)
--     Open date: AM00_DATE_ACCOUNT_OPEN (YYYYDDD). Balance at start: AM02_BALANCE_CURRENT.
--     Join to evaluation on LedgerAccountId = MAST_ACCOUNT_ID.
-- If the AM00 view has a snapshot/partition date column, add: WHERE snapshot_date <= cri_evaluation_date
-- and QUALIFY ROW_NUMBER() OVER (PARTITION BY MAST_ACCOUNT_ID ORDER BY snapshot_date DESC) = 1
-- Parameters:  {dag_id}, {env}

INSERT INTO `pcb-{env}-landing.domain_cri_compliance.CRI_STG_ACCOUNT_MASTER`
  (
    AccountId,
    ProductType,
    FirstDayOfEvaluation,
    LastDayOfEvaluation,
    OpenDate,
    BalanceAtStart,
    RecLoadTimestamp,
    JobId)
WITH
  am00_am02_joined AS (
    SELECT
      am00.MAST_ACCOUNT_ID,
      am00.MAST_ACCOUNT_SUFFIX,
      DATE_ADD(
        DATE(
          CAST(
            DIV(
              COALESCE(
                NULLIF(
                  SAFE_CAST(
                    TRIM(CAST(am00.AM00_DATE_ACCOUNT_OPEN AS STRING)) AS INT64),
                  0),
                2000001),
              1000)
            AS INT64),
          1,
          1),
        INTERVAL
          MOD(
            COALESCE(
              NULLIF(
                SAFE_CAST(
                  TRIM(CAST(am00.AM00_DATE_ACCOUNT_OPEN AS STRING)) AS INT64),
                0),
              2000001),
            1000)
          - 1 DAY) AS OpenDate,
      am02.AM02_BALANCE_CURRENT AS BalanceAtStart
    FROM
      `pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM00_REC`
        am00
    JOIN
      `pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM02_REC`
        am02
      ON
        am00.MAST_ACCOUNT_ID = am02.MAST_ACCOUNT_ID
        AND am00.MAST_ACCOUNT_SUFFIX = am02.AM00_APPLICATION_SUFFIX
    WHERE am00.MAST_ACCOUNT_SUFFIX = 0
  )
SELECT DISTINCT
  e.InternalAccountId AS AccountId,
  CAST('CREDIT' AS STRING) AS ProductType,
  FORMAT_DATE(
    '%Y-%m-%d',
    GREATEST(
      DATE_SUB(SAFE.PARSE_DATE('%Y-%m-%d', e.EventDate), INTERVAL 365 DAY),
      COALESCE(am.OpenDate, DATE('1900-01-01')))) AS FirstDayOfEvaluation,
  FORMAT_DATE('%Y-%m-%d', SAFE.PARSE_DATE('%Y-%m-%d', e.EventDate))
    AS LastDayOfEvaluation,
  FORMAT_DATE(
    '%Y-%m-%d', COALESCE(am.OpenDate, SAFE.PARSE_DATE('%Y-%m-%d', e.EventDate)))
    AS OpenDate,
  CAST(COALESCE(am.BalanceAtStart, 0) AS STRING) AS BalanceAtStart,
  CURRENT_DATETIME('America/Toronto') AS RecLoadTimestamp,
  '{dag_id}' AS JobId
FROM `pcb-{env}-curated.domain_cri_compliance.ACCOUNT_CRI_EVALUATION_LATEST` e
LEFT JOIN am00_am02_joined am
  ON e.LedgerAccountId = am.MAST_ACCOUNT_ID
WHERE
  e.InternalAccountId IS NOT NULL;