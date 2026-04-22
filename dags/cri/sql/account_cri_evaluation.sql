-- CRI: Insert accounts for evaluation into ACCOUNT_CRI_EVALUATION
-- Append-only: each run inserts a new batch differentiated by RecLoadTimestamp and JobId.
-- Data is viewable by load time via ACCOUNT_CRI_EVALUATION_ALL and ACCOUNT_CRI_EVALUATION_LATEST.
--
-- Sources:
--   - pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM00_REC (account master; use MAST_ACCOUNT_SUFFIX = 0)
--   - pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM02_REC (join to AM00 on MAST_ACCOUNT_ID and AM00_APPLICATION_SUFFIX)
--   - pcb-{env}-curated.domain_account_management.ACCOUNT (join to AM00 on MAST_ACCOUNT_ID; internal account ID = ACCOUNT_UID)
--   - pcb-{env}-curated.domain_account_management.PRODUCT (credit-only: TYPE = 'CREDIT-CARD', PRODUCT_HIERARCHY_LEVEL_UID = 4 for PCMC)
--   - pcb-{env}-curated.domain_customer_management.ACCOUNT_CUSTOMER (primary cardholder: ACCOUNT_CUSTOMER_ROLE_UID = 1 → CUSTOMER_UID)
--   - pcb-{env}-curated.domain_customer_management.CUSTOMER
--   - pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER (external ID: TYPE = 'PCF-CUSTOMER-ID', DISABLED_IND = 'N')
--
-- Evaluation runs 5 days after the event: input_date = CURRENT_DATE() - {input_date_offset} days (default 5).
-- Only accounts whose event_date (anniversary or closure with zero balance) = input_date are inserted (i.e. event happened 5 days ago).
-- AM00_DATE_ACCOUNT_OPEN: if null, empty, or 0 then default to 20000101 (Jan 1, 2000) before parsing.
-- Leap year: accounts opened Feb 29 are evaluated on March 1 in non-leap years.
-- Dates in AM00: AM00_DATE_ACCOUNT_OPEN, AM00_DATE_CLOSE are YYYYDDD (integer); parse to DATE.
-- Evaluation threshold: accounts opened on/after 2025-01-01 use 35% APR (0.35); before use 48% APR (0.48).
--
-- Parameters: {input_date_offset}, {dag_id}, {env}
DECLARE input_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL {input_date_offset} DAY);
DECLARE input_year INT64 DEFAULT EXTRACT(YEAR FROM input_date);

INSERT INTO `pcb-{env}-landing.domain_cri_compliance.ACCOUNT_CRI_EVALUATION` (
  CriEvaluationDate,
  InternalAccountId,
  LedgerAccountId,
  Ledger,
  InternalCustomerId,
  ExternalCustomerId,
  Event,
  EventDate,
  EvaluationThresholdApr,
  RecLoadTimestamp,
  JobId
)
WITH
-- Parse YYYYDDD (integer) to DATE: year = DIV(val,1000), day_of_year = MOD(val,1000)
am00_am02_joined AS (
  SELECT
    am00.MAST_ACCOUNT_ID,
    am02.AM00_APPLICATION_SUFFIX,
    am00.AM00_DATE_ACCOUNT_OPEN,
    am00.AM00_DATE_CLOSE,
    am02.AM02_BALANCE_CURRENT,
    DATE_ADD(
      DATE(CAST(DIV(COALESCE(NULLIF(SAFE_CAST(TRIM(CAST(am00.AM00_DATE_ACCOUNT_OPEN AS STRING)) AS INT64), 0), 2000001), 1000) AS INT64), 1, 1),
      INTERVAL MOD(COALESCE(NULLIF(SAFE_CAST(TRIM(CAST(am00.AM00_DATE_ACCOUNT_OPEN AS STRING)) AS INT64), 0), 2000001), 1000) - 1 DAY
    ) AS OpenDate,
    CASE
      WHEN am00.AM00_DATE_CLOSE IS NOT NULL AND am00.AM00_DATE_CLOSE > 0 THEN
        DATE_ADD(
          SAFE.DATE(CAST(DIV(am00.AM00_DATE_CLOSE, 1000) AS INT64), 1, 1),
          INTERVAL MOD(am00.AM00_DATE_CLOSE, 1000) - 1 DAY
        )
      ELSE CAST(NULL AS DATE)
    END AS CloseDate,
    -- Zero balance at closure: use AM02_BALANCE_CURRENT = 0
    (am02.AM02_BALANCE_CURRENT = 0) AS IsZeroBalance
  FROM
    `pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM00_REC` AS am00
  JOIN
    `pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM02_REC` AS am02
    ON am00.MAST_ACCOUNT_ID = am02.MAST_ACCOUNT_ID
    AND am00.MAST_ACCOUNT_SUFFIX = am02.AM00_APPLICATION_SUFFIX
  WHERE
    am00.MAST_ACCOUNT_SUFFIX = 0
),
credit_accounts AS (
  SELECT
    a.ACCOUNT_UID,
    m.MAST_ACCOUNT_ID,
    m.OpenDate,
    m.CloseDate,
    m.IsZeroBalance,
    -- Anniversary in the year of input_date; Feb 29 open date → March 1 in non-leap years
    CASE
      WHEN EXTRACT(MONTH FROM m.OpenDate) = 2 AND EXTRACT(DAY FROM m.OpenDate) = 29 THEN
        IF(
          (MOD(input_year, 4) = 0 AND MOD(input_year, 100) != 0) OR (MOD(input_year, 400) = 0),
          DATE(input_year, 2, 29), DATE(input_year, 3, 1)
        )
      ELSE
        DATE(input_year, EXTRACT(MONTH FROM m.OpenDate), EXTRACT(DAY FROM m.OpenDate))
    END AS AnniversaryDate
  FROM am00_am02_joined m
  INNER JOIN `pcb-{env}-curated.domain_account_management.ACCOUNT` a
    ON a.MAST_ACCOUNT_ID = m.MAST_ACCOUNT_ID
  INNER JOIN `pcb-{env}-curated.domain_account_management.PRODUCT` p
    ON a.PRODUCT_UID = p.PRODUCT_UID
  WHERE
    UPPER(p.TYPE) = 'CREDIT-CARD'
    AND p.PRODUCT_HIERARCHY_LEVEL_UID = 4
),
primary_customer AS (
  SELECT
    ac.ACCOUNT_UID,
    ac.CUSTOMER_UID
  FROM `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` ac
  WHERE ac.ACCOUNT_CUSTOMER_ROLE_UID = 1
),
external_customer_id AS (
  SELECT
    ci.CUSTOMER_UID,
    ci.CUSTOMER_IDENTIFIER_NO
  FROM `pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` ci
  WHERE UPPER(ci.TYPE) = 'PCF-CUSTOMER-ID'
    AND COALESCE(ci.DISABLED_IND, '') = 'N'
),
accounts_for_event_date AS (
  SELECT
    CAST(ca.ACCOUNT_UID AS STRING)   AS InternalAccountId,
    ca.MAST_ACCOUNT_ID                AS LedgerAccountId,
    'TSYS'                            AS Ledger,
    CAST(pc.CUSTOMER_UID AS STRING)   AS InternalCustomerId,
    eci.CUSTOMER_IDENTIFIER_NO        AS ExternalCustomerId,
    CASE
      WHEN ca.CloseDate = input_date AND ca.IsZeroBalance THEN 'closure'
      ELSE 'anniversary'
    END AS Event,
    FORMAT_DATE('%Y-%m-%d', input_date) AS EventDate,
    -- Evaluation threshold APR: 35% for accounts opened on/after 2025-01-01; 48% for before
    CASE
      WHEN ca.OpenDate >= DATE('2025-01-01') THEN '0.35'
      ELSE '0.48'
    END AS EvaluationThresholdApr
  FROM credit_accounts ca
  INNER JOIN primary_customer pc ON ca.ACCOUNT_UID = pc.ACCOUNT_UID
  LEFT JOIN external_customer_id eci ON pc.CUSTOMER_UID = eci.CUSTOMER_UID
  WHERE
    (
      -- Anniversary: account still open and anniversary matches input_date
      (ca.CloseDate IS NULL AND ca.AnniversaryDate = input_date)
      OR
      -- Closure: closed on input_date with zero balance
      (ca.CloseDate = input_date AND ca.IsZeroBalance)
    )
)
SELECT
  FORMAT_DATE('%Y-%m-%d', CURRENT_DATE()) AS CriEvaluationDate,
  InternalAccountId,
  LedgerAccountId,
  Ledger,
  InternalCustomerId,
  ExternalCustomerId,
  Event,
  EventDate,
  EvaluationThresholdApr,
  CURRENT_DATETIME('America/Toronto') AS RecLoadTimestamp,
  '{dag_id}' AS JobId
FROM accounts_for_event_date;
