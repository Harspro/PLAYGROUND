INSERT INTO
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0234`
WITH
  pcma_cdic_0234_latest_load AS (
  SELECT
    MAX( pcma_cdic_0234.REC_LOAD_TIMESTAMP ) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0234` AS pcma_cdic_0234 )
SELECT
  pcma_cdic_0234.INSURANCE_DETERMINATION_CATEGORY_TYPE_CODE,
  pcma_cdic_0234.DESCRIPTION,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0234` AS pcma_cdic_0234
INNER JOIN
  pcma_cdic_0234_latest_load AS pcma_cdic_0234_ll
ON
  pcma_cdic_0234.REC_LOAD_TIMESTAMP = pcma_cdic_0234_ll.LATEST_REC_LOAD_TIMESTAMP;