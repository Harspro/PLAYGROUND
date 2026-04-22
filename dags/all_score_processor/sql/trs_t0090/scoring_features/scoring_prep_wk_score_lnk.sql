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

INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_SCORE_LNK`
(
  MAST_ACCOUNT_ID,
  APP_NUMBER,
  OPEN_DT,
  ORGN_CB_SCORE,
  ORGN_BNI_SCORE,
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
WITH
  MTH_TSYS_EXPERIAN_LNK AS (
    SELECT DISTINCT
      mast_account_id,
      app_number,
      FORMAT_DATE('%Y-%m-%d', parse_date("%d%b%y", open_dt)) AS open_dt
    FROM
      `pcb-{env}-curated.domain_scoring.SCORING_PREP_AM00_PRE_LATEST`
    UNION DISTINCT
    SELECT DISTINCT
      CAST(mast_account_id AS string),
      am00_custom_data_81 AS app_number,
      FORMAT_DATE('%Y-%m-%d', DATE(am00_date_account_open)) AS open_dt
    FROM
      `pcb-{env}-curated.domain_account_management.AM00`
    WHERE
      am00_application_suffix = 0
      AND am00_custom_data_81 IS NOT NULL AND TRIM(am00_custom_data_81) != ''
      AND DATE_TRUNC(file_create_dt, MONTH)
        = DATE_TRUNC(am00_date_account_open, MONTH)
  )
SELECT DISTINCT
  experian_link.mast_account_id AS MAST_ACCOUNT_ID,
  experian_link.app_number AS APP_NUMBER,
  experian_link.open_dt AS OPEN_DT,
  wk_acquisition.orgn_cb_score AS ORGN_CB_SCORE,
  wk_acquisition.orgn_bni_score AS ORGN_BNI_SCORE,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  MTH_TSYS_EXPERIAN_LNK experian_link
LEFT JOIN
  `pcb-{env}-curated.domain_scoring.SCORING_PREP_WK_ACQUISITION_LATEST`
    wk_acquisition
  ON
    experian_link.app_number = wk_acquisition.app_number
WHERE
  experian_link.open_dt < '2015-03-29';
