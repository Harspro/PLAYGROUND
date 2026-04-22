-- Pull CLI data from AM0BH, EVENTS and TRIAD score from SC01 to populate pcmc data depot
BEGIN
DECLARE report_month DATE;
SET report_month = (
  SELECT (get_report_month).report_month
FROM (SELECT `pcb-{env}-landing.domain_scoring.get_report_month`('{report_year}', '{report_month}') AS get_report_month)
);

  CREATE TEMP TABLE MTH_TSYS_EXPERIAN_LNK AS
SELECT DISTINCT
  mast_account_id,
  app_number,
  FORMAT_DATE('%Y-%m-%d',parse_date("%d%b%y", open_dt)) AS open_dt
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_AM00_PRE`
UNION DISTINCT
SELECT DISTINCT
  CAST(mast_account_id AS string),
  am00_custom_data_81 AS app_number,
  FORMAT_DATE('%Y-%m-%d',DATE(am00_date_account_open)) AS open_dt
FROM
  `pcb-{env}-curated.domain_account_management.AM00`
WHERE
  am00_application_suffix=0
  AND NULLIF(TRIM(am00_custom_data_81),"") IS NOT NULL
  AND DATE_TRUNC(file_create_dt, MONTH) = DATE_TRUNC(am00_date_account_open, MONTH)
ORDER BY
  app_number,
  mast_account_id;

  CREATE TEMP TABLE WK_SCORE_LNK AS
SELECT DISTINCT
  experian_link.mast_account_id AS MAST_ACCOUNT_ID,
  experian_link.app_number AS APP_NUMBER,
  experian_link.open_dt AS OPEN_DT,
  wk_acustion.orgn_cb_score AS ORGN_CB_SCORE,
  wk_acustion.orgn_bni_score AS ORGN_BNI_SCORE
FROM
  MTH_TSYS_EXPERIAN_LNK experian_link
LEFT JOIN
  `pcb-{env}-landing.domain_scoring.WK_ACQUISITION` wk_acustion
ON
  CAST(experian_link.app_number AS STRING) = CAST(wk_acustion.app_number AS STRING)
ORDER BY
  APP_NUMBER,
  MAST_ACCOUNT_ID;

CREATE OR REPLACE TABLE `pcb-{env}-landing.domain_scoring.WK_EXPERIAN_SCORE` AS
SELECT DISTINCT
  *
FROM
  WK_SCORE_LNK
WHERE
  open_dt < '2015-03-29';

CREATE OR REPLACE TABLE `pcb-{env}-landing.domain_scoring.WK_CLI_OFFER_DATA` AS
SELECT DISTINCT
  mast_account_id AS MAST_ACCOUNT_ID,
  DATE_TRUNC(am0b_date_behavior_score, MONTH) AS PROCESS_DT,
  am0b_date_behavior_score AS OFFER_DT,
  am0b_cu_reporting_limit_1 AS CURR_LIMIT,
  am0b_amt_last_cl_offer AS OFFER_LIMIT,
  1 AS OFFER
FROM
  `pcb-{env}-curated.domain_account_management.AM0BH`
WHERE
  file_create_dt = report_month
  AND am00_application_suffix = 0
  AND DATE_TRUNC(AM0B_DATE_LAST_CL_OFFER, MONTH) = DATE_TRUNC(file_create_dt, MONTH)
ORDER BY
  mast_account_id;

CREATE OR REPLACE TABLE `pcb-{env}-landing.domain_scoring.WK_CLI_EVENTS` AS
SELECT DISTINCT
  ev3_am00_account_id AS MAST_ACCOUNT_ID,
  DATE_TRUNC(file_create_dt, MONTH) AS PROCESS_DT,
  DATE(ev3_el01_julian_date) AS CLI_DT
FROM
  `pcb-{env}-curated.domain_account_management.EVENTS`
WHERE
  DATE_TRUNC(file_create_dt, MONTH) = DATE_TRUNC(report_month, MONTH)
  AND ev3_el01_type_event="110"
  AND ev3_event_field_num=1
  AND SAFE.PARSE_NUMERIC(ev3_event_old_data) < SAFE.PARSE_NUMERIC(ev3_event_new_data) ;
  -- Pull Triad score
  CREATE TEMP TABLE wk_raw_score_pull AS
SELECT DISTINCT
  mast_account_id,
  sc01_score_raw AS behv_score_raw,
  DATE(sc01_date_score_added) AS score_dt,
  DATE_TRUNC(file_create_dt, MONTH) AS process_dt
FROM
  `pcb-{env}-curated.domain_account_management.SC01`
WHERE
  file_create_dt = report_month
  AND sc00_application_suffix=0
  AND sc01_score_type IN ('T0001')
  AND sc01_customer_type=0
ORDER BY
  mast_account_id,
  process_dt,
  score_dt;

  CREATE OR REPLACE TABLE `pcb-{env}-landing.domain_scoring.WK_RAW_SCORE` AS
SELECT DISTINCT
  mast_account_id AS MAST_ACCOUNT_ID,
  process_dt AS PROCESS_DT,
  score_dt AS SCORE_DT,
  behv_score_raw AS BEHV_SCORE_RAW
FROM (
  SELECT DISTINCT
    *,
    ROW_NUMBER() OVER (PARTITION BY mast_account_id, process_dt ORDER BY score_dt) AS rn
  FROM
    wk_raw_score_pull
  WHERE
    DATE_TRUNC(score_dt, MONTH) = process_dt )
WHERE
  rn = 1
ORDER BY
  1,
  2 ;
END
  ;