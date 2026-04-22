INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_DEMOSTATS`
(
  CODE,
  GEO,
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
SELECT DISTINCT
  -- Only materialize join key and filter field
  CAST(CODE AS STRING) AS CODE,
  CAST(GEO AS STRING) AS GEO,

  -- Audit fields (generated at insert time)
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM `{creditrisk-scoring-project}.{creditrisk-scoring-dataset}.DEMOSTATS_2025`
WHERE UPPER(GEO) = 'FSALDU';  -- Filter matches source query logic