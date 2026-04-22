INSERT INTO
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0231`
WITH
  pcma_cdic_0231_latest_load AS (
  SELECT
    MAX( pcma_cdic_0231.REC_LOAD_TIMESTAMP ) AS LATEST_DATE
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0231` AS pcma_cdic_0231 )
SELECT
  pcma_cdic_0231.PRODUCT_CODE,
  pcma_cdic_0231.MI_PRODUCT_CODE,
  pcma_cdic_0231.DESCRIPTION,
  pcma_cdic_0231.CDIC_PRODUCT_GROUP_CODE,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0231` AS pcma_cdic_0231
INNER JOIN
  pcma_cdic_0231_latest_load AS pcma_cdic_0231_ll
ON
  pcma_cdic_0231.REC_LOAD_TIMESTAMP = pcma_cdic_0231_ll.LATEST_DATE;