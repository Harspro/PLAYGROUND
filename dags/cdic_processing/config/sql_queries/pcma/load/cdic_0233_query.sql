INSERT INTO
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0233`
WITH
  pcma_cdic_0233_latest_load AS (
  SELECT
    MAX(pcma_cdic_0233.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0233` AS pcma_cdic_0233 )
SELECT
  pcma_cdic_0233.CURRENCY_CODE,
  pcma_cdic_0233.MI_CURRENCY_CODE,
  pcma_cdic_0233.ISO_CURRENCY_CODE,
  pcma_cdic_0233.DESCRIPTION,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0233` AS pcma_cdic_0233
INNER JOIN
  pcma_cdic_0233_latest_load AS pcma_cdic_0233_ll
ON
  pcma_cdic_0233.REC_LOAD_TIMESTAMP = pcma_cdic_0233_ll.LATEST_REC_LOAD_TIMESTAMP;