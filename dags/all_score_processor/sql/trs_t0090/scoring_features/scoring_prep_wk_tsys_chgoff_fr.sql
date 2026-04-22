INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_TSYS_CHGOFF_FR`
(
  MAST_ACCOUNT_ID,
  PROCESS_DT,
  FR_AMT,
  FR_CNT,
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
SELECT DISTINCT
  wk_tsys_chgoff_full_latest.MAST_ACCOUNT_ID,
  wk_tsys_chgoff_full_latest.PROCESS_DT,
  CAST(SUM(CAST(wk_tsys_chgoff_full_latest.FR_AMT AS FLOAT64)) AS STRING) AS FR_AMT,
  CAST(COUNT(wk_tsys_chgoff_full_latest.FR_AMT) AS STRING) AS FR_CNT,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_scoring.SCORING_PREP_WK_TSYS_CHGOFF_FULL_LATEST` AS wk_tsys_chgoff_full_latest
WHERE
  wk_tsys_chgoff_full_latest.FR_AMT IS NOT NULL
GROUP BY
  1,
  2;
