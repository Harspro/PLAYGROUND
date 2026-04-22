INSERT INTO
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0212`
WITH
  pcma_cdic_0212_latest_load AS (
  SELECT
    MAX(pcma_cdic_0212.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0212` AS pcma_cdic_0212 )
SELECT
  pcma_cdic_0212.CDIC_PERSONAL_ID_TYPE_CODE,
  pcma_cdic_0212.DESCRIPTION,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0212` AS pcma_cdic_0212
INNER JOIN
  pcma_cdic_0212_latest_load AS pcma_cdic_0212_ll
ON
  pcma_cdic_0212.REC_LOAD_TIMESTAMP = pcma_cdic_0212_ll.LATEST_REC_LOAD_TIMESTAMP;