INSERT INTO
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0201`
WITH
  pcma_cdic_0201_latest_load AS (
  SELECT
    MAX(pcma_cdic_0201.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0201` AS pcma_cdic_0201 )
SELECT
  pcma_cdic_0201.DEPOSITOR_TYPE_CODE,
  pcma_cdic_0201.MI_DEPOSITOR_TYPE,
  pcma_cdic_0201.DESCRIPTION,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0201` AS pcma_cdic_0201
INNER JOIN
  pcma_cdic_0201_latest_load AS pcma_cdic_0201_ll
ON
  pcma_cdic_0201.REC_LOAD_TIMESTAMP = pcma_cdic_0201_ll.LATEST_REC_LOAD_TIMESTAMP;