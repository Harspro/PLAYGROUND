INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_TSYS_CHGOFF_SUFFIX`
(
  MAST_ACCOUNT_ID,
  PROCESS_DT,
  BK_AMT_SUFFIX,
  BK_CNT_SUFFIX,
  CO_AMT_SUFFIX,
  CO_CNT_SUFFIX,
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
SELECT DISTINCT
  wk_tsys_chgoff_full.MAST_ACCOUNT_ID,
  wk_tsys_chgoff_full.PROCESS_DT,
  CAST(SUM(CAST(wk_tsys_chgoff_full.BK_AMT AS FLOAT64)) AS STRING)
    AS BK_AMT_SUFFIX,
  CAST(COUNT(wk_tsys_chgoff_full.BK_AMT) AS STRING) AS BK_CNT_SUFFIX,
  CAST(SUM(CAST(wk_tsys_chgoff_full.CO_AMT AS FLOAT64)) AS STRING)
    AS CO_AMT_SUFFIX,
  CAST(COUNT(wk_tsys_chgoff_full.CO_AMT) AS STRING) AS CO_CNT_SUFFIX,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_scoring.SCORING_PREP_WK_TSYS_CHGOFF_FULL_LATEST`
    AS wk_tsys_chgoff_full
WHERE CAST(wk_tsys_chgoff_full.AM00_APPLICATION_SUFFIX AS INT64) > 0
GROUP BY
  1,
  2;
