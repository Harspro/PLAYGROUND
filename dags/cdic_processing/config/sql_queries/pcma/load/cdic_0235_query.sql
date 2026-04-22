INSERT INTO
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0235`
WITH
  pcma_cdic_0235_latest_load AS (
  SELECT
    MAX(pcma_cdic_0235.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0235` AS pcma_cdic_0235 )
SELECT
  pcma_cdic_0235.CDIC_HOLD_STATUS_CODE,
  pcma_cdic_0235.CDIC_HOLD_STATUS,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0235` AS pcma_cdic_0235
INNER JOIN
  pcma_cdic_0235_latest_load AS pcma_cdic_0235_ll
ON
  pcma_cdic_0235.REC_LOAD_TIMESTAMP = pcma_cdic_0235_ll.LATEST_REC_LOAD_TIMESTAMP;