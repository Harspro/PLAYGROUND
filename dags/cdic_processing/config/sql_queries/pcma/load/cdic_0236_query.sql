INSERT INTO
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0236`
WITH
  pcma_cdic_0236_latest_load AS (
  SELECT
    MAX( pcma_cdic_0236.REC_LOAD_TIMESTAMP ) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0236` AS pcma_cdic_0236 )
SELECT
  pcma_cdic_0236.ACCOUNT_STATUS_CODE,
  pcma_cdic_0236.MI_ACCOUNT_STATUS_CODE,
  pcma_cdic_0236.DESCRIPTION,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0236` AS pcma_cdic_0236
INNER JOIN
  pcma_cdic_0236_latest_load AS pcma_cdic_0236_ll
ON
  pcma_cdic_0236.REC_LOAD_TIMESTAMP = pcma_cdic_0236_ll.LATEST_REC_LOAD_TIMESTAMP;