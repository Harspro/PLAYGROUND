INSERT INTO
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0242`
SELECT
  '' AS ISO_Currency_Code,
  '' AS Foreign_Currency_CAD_FX,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0242` AS pcma_cdic_0242;