BEGIN
  DECLARE report_month DATE;

SET report_month = (
  SELECT (get_report_month).report_month
  FROM
    (
      SELECT
        `pcb-{env}-landing.domain_scoring.get_report_month`(
          '{report_year}', '{report_month}')
          AS get_report_month
    )
);

INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_TSYS_AM02`
(
  FILE_CREATE_DT,
  MAST_ACCOUNT_ID,
  AM00_APPLICATION_SUFFIX,
  MTHEND_BAL,
  LST_MRCH_DT,
  LST_CASH_DT,
  LST_PYMT_DT,
  LST_CRED_DT,
  FST_ACTV_DT,
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
SELECT DISTINCT
  CAST(file_create_dt AS STRING) AS FILE_CREATE_DT,
  CAST(mast_account_id AS STRING) AS MAST_ACCOUNT_ID,
  CAST(am00_application_suffix AS STRING) AS AM00_APPLICATION_SUFFIX,
  CAST(am02_balance_current AS STRING) AS MTHEND_BAL,
  CAST(DATE(am02_date_last_purchase) AS STRING) AS LST_MRCH_DT,
  CAST(DATE(am02_date_last_cash) AS STRING) AS LST_CASH_DT,
  CAST(DATE(am02_date_last_payment) AS STRING) AS LST_PYMT_DT,
  CAST(DATE(am02_date_last_credit) AS STRING) AS LST_CRED_DT,
  CAST(DATE(am02_date_first_use) AS STRING) AS FST_ACTV_DT,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_account_management.AM02`
WHERE
  file_create_dt = report_month;

END
