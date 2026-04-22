INSERT INTO
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0999`
WITH
  pcma_cdic_0999_latest_load AS (
  SELECT
    MAX(pcma_cdic_0999.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0999` AS pcma_cdic_0999 )
SELECT
  pcma_cdic_0999.SUBSYSTEM_ID, 
  pcma_cdic_0999.MI_SUBSYSTEM_CODE, 
  pcma_cdic_0999.DESCRIPTION,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0999` AS pcma_cdic_0999
INNER JOIN
  pcma_cdic_0999_latest_load AS pcma_cdic_0999_ll
ON
  pcma_cdic_0999.REC_LOAD_TIMESTAMP = pcma_cdic_0999_ll.LATEST_REC_LOAD_TIMESTAMP;