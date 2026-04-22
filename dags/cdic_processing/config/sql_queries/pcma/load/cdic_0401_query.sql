INSERT INTO
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0401`
WITH
  pcma_cdic_0401_latest_load AS (
  SELECT
    MAX( pcma_cdic_0401.REC_LOAD_TIMESTAMP ) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0401` AS pcma_cdic_0401 )
SELECT
  pcma_cdic_0401.TRANSACTION_CODE,
  pcma_cdic_0401.MI_TRANSACTION_CODE,
  pcma_cdic_0401.DESCRIPTION,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0401` AS pcma_cdic_0401
INNER JOIN
  pcma_cdic_0401_latest_load AS pcma_cdic_0401_ll
ON
  pcma_cdic_0401.REC_LOAD_TIMESTAMP = pcma_cdic_0401_ll.LATEST_REC_LOAD_TIMESTAMP;