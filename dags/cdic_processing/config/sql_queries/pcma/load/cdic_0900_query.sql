INSERT INTO
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0900`
WITH
  pcma_cdic_0999_latest_load AS (
  SELECT
    MAX(pcma_cdic_0999.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0999` AS pcma_cdic_0999 ),
  pcma_cdic_0999_subsystem_unique_result AS (
  SELECT
    cdic_0999.SUBSYSTEM_ID
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0999` AS cdic_0999
  INNER JOIN
    pcma_cdic_0999_latest_load AS pcma_cdic_0999_ll
  ON
    cdic_0999.REC_LOAD_TIMESTAMP = pcma_cdic_0999_ll.LATEST_REC_LOAD_TIMESTAMP
  WHERE
    LOWER(MI_SUBSYSTEM_CODE) = 'pcb-deposits-1'),
  pcma_cdic_0233_latest_load AS (
  SELECT
    MAX( pcma_cdic_0233.REC_LOAD_TIMESTAMP ) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0233` AS pcma_cdic_0233 ),
  pcma_cdic_0233_currency_unique_result AS (
  SELECT
    cdic_0233.CURRENCY_CODE
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0233` AS cdic_0233
  INNER JOIN
    pcma_cdic_0233_latest_load AS pcma_cdic_0233_ll
  ON
    cdic_0233.REC_LOAD_TIMESTAMP = pcma_cdic_0233_ll.LATEST_REC_LOAD_TIMESTAMP
  WHERE
    LOWER(MI_CURRENCY_CODE) = 'cad'),
  pcma_cdic_0500_latest_load AS (
  SELECT
    MAX(pcma_cdic_0500.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0500` AS pcma_cdic_0500 )
SELECT
  DISTINCT CDIC_0500.ACCOUNT_UNIQUE_ID,
  (
  SELECT
    pcma_cdic_0999.SUBSYSTEM_ID
  FROM
    pcma_cdic_0999_subsystem_unique_result pcma_cdic_0999 ) AS SUBSYSTEM_ID,
  '' AS LAST_INTEREST_PAYMENT_DATE,
  '0.0000' AS INTEREST_ACCRUED_AMOUNT,
  (
  SELECT
    pcma_cdic_0233.CURRENCY_CODE
  FROM
    pcma_cdic_0233_currency_unique_result AS pcma_cdic_0233) AS CURRENCY_CODE,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0500` AS cdic_0500
INNER JOIN
  pcma_cdic_0500_latest_load AS pcma_cdic_500_ll
ON
  cdic_0500.REC_LOAD_TIMESTAMP = pcma_cdic_500_ll.LATEST_REC_LOAD_TIMESTAMP