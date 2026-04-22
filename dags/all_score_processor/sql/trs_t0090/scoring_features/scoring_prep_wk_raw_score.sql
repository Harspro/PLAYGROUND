-- Pull Triad score
DECLARE report_month DATE;

SET report_month = (
  SELECT (get_report_month).report_month
  FROM
    (
      SELECT
        `pcb-{env}-landing.domain_scoring.get_report_month`(
          '{report_year}', '{report_month}') AS get_report_month
    )
);

INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_RAW_SCORE`
(
  MAST_ACCOUNT_ID,
  PROCESS_DT,
  SCORE_DT,
  BEHV_SCORE_RAW,
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
SELECT DISTINCT
  CAST(mast_account_id AS STRING) AS MAST_ACCOUNT_ID,
  FORMAT_DATE('%Y-%m-%d', process_dt) AS PROCESS_DT,
  FORMAT_DATE('%Y-%m-%d', score_dt) AS SCORE_DT,
  CAST(behv_score_raw AS STRING) AS BEHV_SCORE_RAW,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  (
    SELECT DISTINCT
      mast_account_id,
      sc01_score_raw AS behv_score_raw,
      DATE(sc01_date_score_added) AS score_dt,
      DATE_TRUNC(file_create_dt, MONTH) AS process_dt,
      ROW_NUMBER()
        OVER (
          PARTITION BY mast_account_id, DATE_TRUNC(file_create_dt, MONTH)
          ORDER BY DATE(sc01_date_score_added)
        ) AS rn
    FROM
      `pcb-{env}-curated.domain_account_management.SC01`
    WHERE
      file_create_dt = report_month
      AND sc00_application_suffix = 0
      AND sc01_score_type IN ('T0001')
      AND sc01_customer_type = 0
      AND DATE_TRUNC(DATE(sc01_date_score_added), MONTH)
        = DATE_TRUNC(file_create_dt, MONTH)
  )
WHERE
  rn = 1;
