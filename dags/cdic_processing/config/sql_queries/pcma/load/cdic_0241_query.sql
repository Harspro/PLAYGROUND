INSERT INTO
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0241`
WITH
  pcma_cdic_0241_latest_load AS (
  SELECT
    MAX(pcma_cdic_0241.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0241` AS pcma_cdic_0241 )
SELECT
  pcma_cdic_0241.MI_DEPOSIT_HOLD_CODE,
  pcma_cdic_0241.MI_DEPOSIT_HOLD_TYPE,
  pcma_cdic_0241.DESCRIPTION,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0241` AS pcma_cdic_0241
INNER JOIN
  pcma_cdic_0241_latest_load AS pcma_cdic_0241_ll
ON
  pcma_cdic_0241.REC_LOAD_TIMESTAMP = pcma_cdic_0241_ll.LATEST_REC_LOAD_TIMESTAMP;