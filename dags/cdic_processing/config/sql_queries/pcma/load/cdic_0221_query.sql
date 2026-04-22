INSERT INTO
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0221`
WITH
  pcma_cdic_0221_latest_load AS (
  SELECT
    MAX(pcma_cdic_0221.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0221` AS pcma_cdic_0221 )
SELECT
  pcma_cdic_0221.ADDRESS_TYPE_CODE, 
  pcma_cdic_0221.MI_ADDRESS_TYPE, 
  pcma_cdic_0221.DESCRIPTION,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0221` AS pcma_cdic_0221
INNER JOIN
  pcma_cdic_0221_latest_load AS pcma_cdic_0221_ll
ON
  pcma_cdic_0221.REC_LOAD_TIMESTAMP = pcma_cdic_0221_ll.LATEST_REC_LOAD_TIMESTAMP;