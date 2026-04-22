INSERT INTO
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0232`
WITH
  pcma_cdic_0232_latest_load AS (
  SELECT
    MAX(pcma_cdic_0232.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0232` AS pcma_cdic_0232 )
SELECT
  pcma_cdic_0232.REGISTERED_PLAN_TYPE_CODE,
  pcma_cdic_0232.MI_REGISTERED_PLAN_TYPE,
  pcma_cdic_0232.DESCRIPTION,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0232` AS pcma_cdic_0232
INNER JOIN
  pcma_cdic_0232_latest_load AS pcma_cdic_0232_ll
ON
  pcma_cdic_0232.REC_LOAD_TIMESTAMP = pcma_cdic_0232_ll.LATEST_REC_LOAD_TIMESTAMP;