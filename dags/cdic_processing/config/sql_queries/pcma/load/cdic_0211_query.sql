INSERT INTO
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0211`
WITH
  pcma_cdic_0211_latest_load AS (
  SELECT
    MAX(pcma_cdic_0211.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0211` AS pcma_cdic_0211 )
SELECT
  pcma_cdic_0211.PERSONAL_ID_TYPE_CODE, 
  pcma_cdic_0211.MI_PERSONAL_ID_TYPE, 
  pcma_cdic_0211.DESCRIPTION, 
  pcma_cdic_0211.CDIC_PERSONAL_ID_TYPE_CODE,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0211` AS pcma_cdic_0211
INNER JOIN
  pcma_cdic_0211_latest_load AS pcma_cdic_0211_ll
ON
  pcma_cdic_0211.REC_LOAD_TIMESTAMP = pcma_cdic_0211_ll.LATEST_REC_LOAD_TIMESTAMP;