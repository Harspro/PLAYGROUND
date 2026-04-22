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

INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_CLI_OFFER_DATA`
(
  MAST_ACCOUNT_ID,
  PROCESS_DT,
  OFFER_DT,
  CURR_LIMIT,
  OFFER_LIMIT,
  OFFER,
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
SELECT DISTINCT
  CAST(mast_account_id AS STRING) AS MAST_ACCOUNT_ID,
  FORMAT_DATETIME(
    '%Y-%m-%dT%H:%M:%S', DATE_TRUNC(am0b_date_behavior_score, MONTH))
    AS PROCESS_DT,
  FORMAT_DATETIME('%Y-%m-%dT%H:%M:%S', am0b_date_behavior_score) AS OFFER_DT,
  CAST(am0b_cu_reporting_limit_1 AS STRING) AS CURR_LIMIT,
  CAST(am0b_amt_last_cl_offer AS STRING) AS OFFER_LIMIT,
  '1' AS OFFER,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_account_management.AM0BH`
WHERE
  file_create_dt = report_month
  AND am00_application_suffix = 0
  AND DATE_TRUNC(AM0B_DATE_LAST_CL_OFFER, MONTH)
    = DATE_TRUNC(file_create_dt, MONTH);