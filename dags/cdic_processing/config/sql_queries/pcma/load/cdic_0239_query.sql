INSERT INTO
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0239`
WITH
  pcma_cdic_0239_latest_load AS (
  SELECT
    MAX( pcma_cdic_0239.REC_LOAD_TIMESTAMP ) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0239` AS pcma_cdic_0239 )
SELECT
  pcma_cdic_0239.ACCOUNT_TYPE_CODE,
  pcma_cdic_0239.MI_ACCOUNT_TYPE,
  pcma_cdic_0239.DESCRIPTION,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0239` AS pcma_cdic_0239
INNER JOIN
   pcma_cdic_0239_latest_load AS pcma_cdic_0239_ll
ON
  pcma_cdic_0239.REC_LOAD_TIMESTAMP = pcma_cdic_0239_ll.LATEST_REC_LOAD_TIMESTAMP;
