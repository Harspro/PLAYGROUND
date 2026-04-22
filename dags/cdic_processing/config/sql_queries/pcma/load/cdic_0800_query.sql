INSERT INTO
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0800`
WITH
  pcma_cdic_0999_latest_load AS (
  SELECT
    MAX(pcma_cdic_0999.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0999` AS pcma_cdic_0999),
  pcma_cdic_0999_result AS (
  SELECT
    cdic_0999.SUBSYSTEM_ID
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0999` AS cdic_0999
  INNER JOIN
    pcma_cdic_0999_latest_load AS pcma_cdic_0999_ll
  ON
    cdic_0999.REC_LOAD_TIMESTAMP = pcma_cdic_0999_ll.LATEST_REC_LOAD_TIMESTAMP
  WHERE
    LOWER(cdic_0999.MI_SUBSYSTEM_CODE) = 'pcb-deposits-1' ),
  pcma_cdic_0500_latest_load AS (
  SELECT
    MAX(pcma_cdic_0500.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0500` AS pcma_cdic_0500),
  pcma_cdic_0800_result AS (
  SELECT
    DISTINCT cdic_0500.ACCOUNT_UNIQUE_ID
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0500` AS cdic_0500
  INNER JOIN
    pcma_cdic_0500_latest_load AS pcma_cdic_500_ll
  ON
    cdic_0500.REC_LOAD_TIMESTAMP = pcma_cdic_500_ll.LATEST_REC_LOAD_TIMESTAMP ),
  pcma_cdic_0700_latest_load AS (
  SELECT
    MAX(pcma_cdic_0700.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0700` AS pcma_cdic_0700),
  pcma_cdic_0700_result AS (
  SELECT
    pcma_cdic_0700.ACCOUNT_UNIQUE_ID,
    pcma_cdic_0700.HOLD_AMOUNT
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0700` AS pcma_cdic_0700
  INNER JOIN
    pcma_cdic_0700_latest_load AS pcma_cdic_0700_ll
  ON
    IFNULL(pcma_cdic_0700.REC_LOAD_TIMESTAMP,CURRENT_DATETIME()) = IFNULL(pcma_cdic_0700_ll.LATEST_REC_LOAD_TIMESTAMP,CURRENT_DATETIME() ) ),
  cdic_hold_status AS (
  SELECT
    pcma_cdic_0800.ACCOUNT_UNIQUE_ID,
    CASE
      WHEN pcma_cdic_0700.ACCOUNT_UNIQUE_ID IS NULL THEN 1
      ELSE CASE
      WHEN CAST(pcma_cdic_0700.HOLD_AMOUNT AS FLOAT64) > 0 THEN 3
      WHEN CAST(pcma_cdic_0700.HOLD_AMOUNT AS FLOAT64) = 0
    OR CAST(pcma_cdic_0700.HOLD_AMOUNT AS FLOAT64) = -1 THEN 1
      WHEN CAST(pcma_cdic_0700.HOLD_AMOUNT AS FLOAT64) = -2 THEN 2
  END
  END AS CDIC_HOLD_STATUS_CODE,
    IFNULL(CAST(pcma_cdic_0700.HOLD_AMOUNT AS FLOAT64), 0) AS CDIC_HOLD_AMOUNT
  FROM
    pcma_cdic_0800_result AS pcma_cdic_0800
  LEFT JOIN
    pcma_cdic_0700_result AS pcma_cdic_0700
  ON
    pcma_cdic_0800.ACCOUNT_UNIQUE_ID = pcma_cdic_0700.ACCOUNT_UNIQUE_ID),
  pcma_cdic_0233_latest_load AS(
  SELECT
    MAX(pcma_cdic_0233.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0233` AS pcma_cdic_0233),
  pcma_cdic_0233_currency_unique_result AS(
  SELECT
    cdic_0233.CURRENCY_CODE
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0233` AS cdic_0233
  INNER JOIN
    pcma_cdic_0233_latest_load AS pcma_cdic_0233_ll
  ON
    cdic_0233.REC_LOAD_TIMESTAMP = pcma_cdic_0233_ll.LATEST_REC_LOAD_TIMESTAMP
  WHERE
    LOWER(cdic_0233.MI_CURRENCY_CODE) = 'cad'),
  balance AS (
  SELECT
    am00.MAST_ACCOUNT_ID,
    FORMAT('%.2f',COALESCE((am02.AM02_BALANCE_CURRENT*-1),0)) AS ACCOUNT_BALANCE,
    FORMAT('%.2f',(COALESCE((am02.AM02_BALANCE_CURRENT*-1),0) + COALESCE(am00.AM00_LIMIT_CREDIT,0)+ COALESCE(am00.AM00_AUTH_PAYMENT_AMT,0) - COALESCE(am00.AM00_AMT_AUTHORIZATIONS_OUT,0))) AS ACCESSIBLE_BALANCE
  FROM
    `pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM02_REC` AS am02
  INNER JOIN
    `pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM00_REC` AS am00
  ON
    am02.MAST_ACCOUNT_ID = am00.MAST_ACCOUNT_ID
  WHERE
    am00.MAST_ACCOUNT_SUFFIX = 0
    AND am02.AM00_APPLICATION_SUFFIX = 0)
SELECT
  cdic_hold.ACCOUNT_UNIQUE_ID,
  (
  SELECT
    cdic_0999.SUBSYSTEM_ID
  FROM
    pcma_cdic_0999_result cdic_0999) AS SUBSYSTEM_ID,
  CAST(cdic_hold.CDIC_HOLD_STATUS_CODE AS STRING) AS CDIC_HOLD_STATUS_CODE,
  IFNULL(bal.ACCOUNT_BALANCE, '0.0') AS ACCOUNT_BALANCE,
  IFNULL(bal.ACCESSIBLE_BALANCE, '0.0') AS ACCESSIBLE_BALANCE,
  FORMAT('%.2f', CAST(cdic_hold.CDIC_HOLD_AMOUNT AS FLOAT64)) AS CDIC_HOLD_AMOUNT,
  (
  SELECT
    cdic_0233.CURRENCY_CODE
  FROM
    pcma_cdic_0233_currency_unique_result AS cdic_0233) AS CURRENCY_CODE,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  cdic_hold_status AS cdic_hold
LEFT JOIN
  balance AS bal
ON
  LTRIM(CAST(cdic_hold.ACCOUNT_UNIQUE_ID AS string),'0') = LTRIM(CAST(bal.MAST_ACCOUNT_ID AS STRING), '0');