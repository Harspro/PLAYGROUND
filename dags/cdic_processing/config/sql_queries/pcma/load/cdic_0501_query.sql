INSERT INTO
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0501`
WITH
  pcma_cdic_0501_latest_load AS (
  SELECT
    MAX(pcma_cdic_0501.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0501` AS pcma_cdic_0501 )
SELECT
  pcma_cdic_0501.RELATIONSHIP_TYPE_CODE, 
  pcma_cdic_0501.MI_RELATIONSHIP_TYPE, 
  pcma_cdic_0501.DESCRIPTION,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0501` AS pcma_cdic_0501
INNER JOIN
  pcma_cdic_0501_latest_load AS pcma_cdic_0501_ll
ON
  pcma_cdic_0501.REC_LOAD_TIMESTAMP = pcma_cdic_0501_ll.LATEST_REC_LOAD_TIMESTAMP;