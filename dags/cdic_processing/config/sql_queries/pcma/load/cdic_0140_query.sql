INSERT INTO
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0140`
WITH
  pcma_cdic_0241_latest_load AS (
  SELECT
    MAX(pcma_cdic_0241.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0241` AS pcma_cdic_0241 ),
  pcma_cdic_0241_mi_deposit_unique_result AS (
  SELECT
    pcma_cdic_0241.MI_DEPOSIT_HOLD_CODE
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0241` AS pcma_cdic_0241
  INNER JOIN
    pcma_cdic_0241_latest_load AS pcma_cdic_0241_ll
  ON
    pcma_cdic_0241.REC_LOAD_TIMESTAMP = pcma_cdic_0241_ll.LATEST_REC_LOAD_TIMESTAMP
  WHERE
    LOWER(pcma_cdic_0241.MI_DEPOSIT_HOLD_TYPE) = 'fraud'),
  pcma_cdic_0233_latest_load AS (
  SELECT
    MAX(pcma_cdic_0233.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0233` AS pcma_cdic_0233),
  pcma_cdic_0233_currency_unique_result AS (
  SELECT
    pcma_cdic_0233.CURRENCY_CODE
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0233` AS pcma_cdic_0233
  INNER JOIN
    pcma_cdic_0233_latest_load AS pcma_cdic_0233_ll
  ON
    pcma_cdic_0233.REC_LOAD_TIMESTAMP = pcma_cdic_0233_ll.LATEST_REC_LOAD_TIMESTAMP
  WHERE
    LOWER(pcma_cdic_0233.MI_CURRENCY_CODE) = 'cad'),
  pcma_cdic_0500_latest_load AS (
  SELECT
    MAX(pcma_cdic_0500.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0500` AS pcma_cdic_0500 ),
  pcma_cdic_0500_unique_result AS (
  SELECT
    DISTINCT pcma_cdic_0500.ACCOUNT_UNIQUE_ID
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0500` AS pcma_cdic_0500
  INNER JOIN
    pcma_cdic_0500_latest_load AS pcma_cdic_0500_ll
  ON
    pcma_cdic_0500.REC_LOAD_TIMESTAMP = pcma_cdic_0500_ll.LATEST_REC_LOAD_TIMESTAMP ),
  decision_keys_amount AS (
  SELECT
    dec_key.MASTER_ACCOUNT_ID,
    FORMAT_DATE('%Y%m%d', DATE_ADD(dec_key.FILE_CREATE_DT,INTERVAL 7 DAY)) AS MI_DEPOSIT_HOLD_SCHEDULED_RELEASE_DATE,
    FORMAT('%.2f', SAFE_CAST(dec_key.KEY_251 AS FLOAT64)) AS MI_DEPOSIT_HOLD_AMOUNT
  FROM
    `pcb-{env}-curated.domain_account_management.DECISION_KEYS` AS dec_key
  WHERE
    dec_key.KEY_251 IS NOT NULL
    AND (ABS(SAFE_CAST(dec_key.KEY_251 AS FLOAT64)) != 0)
    AND dec_key.KEY_251!=''
    AND (dec_key.KEY_251 LIKE '+%'
      OR dec_key.KEY_251 LIKE '0%')
    AND dec_key.SPID IN (100,
      101,
      600,
      601)
    AND dec_key.Key_279 IN ('+00000018',
      '+00000017')
    AND ( dec_key.FILE_CREATE_DT BETWEEN DATE_SUB(CURRENT_DATE('America/Toronto'), INTERVAL 7 DAY)
      AND CURRENT_DATE('America/Toronto')))
SELECT
  cdic_0500.ACCOUNT_UNIQUE_ID,
  (
  SELECT
    cdic_0241.MI_DEPOSIT_HOLD_CODE
  FROM
    pcma_cdic_0241_mi_deposit_unique_result cdic_0241) AS MI_DEPOSIT_HOLD_CODE,
  held_perc_amt.MI_DEPOSIT_HOLD_SCHEDULED_RELEASE_DATE AS MI_DEPOSIT_HOLD_SCHEDULED_RELEASE_DATE,
  (
  SELECT
    cdic_0233.CURRENCY_CODE
  FROM
    pcma_cdic_0233_currency_unique_result cdic_0233 ) AS CURRENCY_CODE,
  held_perc_amt.MI_DEPOSIT_HOLD_AMOUNT,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  decision_keys_amount AS held_perc_amt
INNER JOIN
  pcma_cdic_0500_unique_result AS cdic_0500
ON
  CAST(cdic_0500.ACCOUNT_UNIQUE_ID AS int64) = held_perc_amt.MASTER_ACCOUNT_ID