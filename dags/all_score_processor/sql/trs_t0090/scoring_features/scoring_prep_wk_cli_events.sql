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

INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_CLI_EVENTS`
(
  MAST_ACCOUNT_ID, 
  PROCESS_DT, 
  CLI_DT, 
  REC_LOAD_TIMESTAMP, 
  JOB_ID
)
SELECT DISTINCT
  CAST(ev3_am00_account_id AS STRING) AS MAST_ACCOUNT_ID,
  FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(file_create_dt, MONTH)) AS PROCESS_DT,
  FORMAT_DATE('%Y-%m-%d', DATE(ev3_el01_julian_date)) AS CLI_DT,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_account_management.EVENTS`
WHERE
  DATE_TRUNC(file_create_dt, MONTH) = DATE_TRUNC(report_month, MONTH)
  AND ev3_el01_type_event = "110"
  AND ev3_event_field_num = 1
  AND SAFE.PARSE_NUMERIC(ev3_event_old_data)
    < SAFE.PARSE_NUMERIC(ev3_event_new_data)
