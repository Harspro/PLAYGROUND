INSERT INTO
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0202`
WITH
  pcma_cdic_0202_latest_load AS (
  SELECT
    MAX(pcma_cdic_0202.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0202` AS pcma_cdic_0202 )
SELECT
  pcma_cdic_0202.PHONE_TYPE_CODE,
  pcma_cdic_0202.DESCRIPTION,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0202` AS pcma_cdic_0202
INNER JOIN
  pcma_cdic_0202_latest_load AS pcma_cdic_0202_ll
ON
  pcma_cdic_0202.REC_LOAD_TIMESTAMP = pcma_cdic_0202_ll.LATEST_REC_LOAD_TIMESTAMP;