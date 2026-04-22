INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_TSYS_MTHEND_SUFFIX`
(
  MAST_ACCOUNT_ID,
  PROCESS_DT,
  MTHEND_BAL_SUFFIX,
  MTHEND_CNT_SUFFIX,
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
SELECT DISTINCT
  tsys_mthend_full_latest.MAST_ACCOUNT_ID,
  tsys_mthend_full_latest.PROCESS_DT,
  CAST(SUM(CAST(tsys_mthend_full_latest.MTHEND_BAL AS FLOAT64)) AS STRING) AS MTHEND_BAL_SUFFIX,
  CAST(COUNT(*) AS STRING) AS MTHEND_CNT_SUFFIX,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_scoring.SCORING_PREP_WK_TSYS_MTHEND_FULL_LATEST` tsys_mthend_full_latest
WHERE
  CAST(tsys_mthend_full_latest.AM00_APPLICATION_SUFFIX AS INT64)>0
  AND tsys_mthend_full_latest.CO_IND IS NULL
  AND tsys_mthend_full_latest.BK_IND IS NULL
  AND tsys_mthend_full_latest.FR_IND IS NULL
GROUP BY
  1,
  2
ORDER BY
  1,
  2 ;