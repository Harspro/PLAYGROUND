INSERT INTO
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0238`
WITH
  pcma_cdic_0238_latest_load AS (
  SELECT
    MAX(pcma_cdic_0238.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0238` AS pcma_cdic_0238 )
SELECT
  pcma_cdic_0238.CLEARING_ACCOUNT_CODE,
  pcma_cdic_0238.MI_CLEARING_ACCOUNT,
  pcma_cdic_0238.DESCRIPTION,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0238` AS pcma_cdic_0238
INNER JOIN
  pcma_cdic_0238_latest_load AS pcma_cdic_0238_ll
ON
  pcma_cdic_0238.REC_LOAD_TIMESTAMP = pcma_cdic_0238_ll.LATEST_REC_LOAD_TIMESTAMP;