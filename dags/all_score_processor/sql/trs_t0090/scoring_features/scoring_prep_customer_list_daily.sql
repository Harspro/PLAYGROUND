DECLARE rundate DATE DEFAULT CURRENT_DATE('America/Toronto');
DECLARE threshold DATE;

SET threshold = date_sub(rundate, INTERVAL 180 day);

INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_CUSTOMER_LIST_DAILY`
(
  FILE_CREATE_DT,
  MAST_ACCOUNT_ID,
  CIFP_TRIAD_SCORE_ALIGNED,
  CIFP_CREDIT_LIMIT,
  CIFP_PREV_STAT_BAL,
  CIFP_DATE_OPEN,
  CIFP_FIRST_USE_DATE,
  CIFP_CLIENT_PRODUCT_CODE,
  CIFP_CURRENT_BALANCE,
  CIFP_AMT_PAY_CTD,
  OPEN_TOB,
  TOB,
  PYMT_RATIO,
  UTIL,
  CIFP_TEST_DIGIT_TYPE5,
  CIFP_ACQUISITION_CODE,
  CIFP_ZIP_CODE,
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
SELECT DISTINCT
  FORMAT_DATE('%Y-%m-%d', acct.file_create_dt) AS file_create_dt,
  CAST(acct.cifp_account_id5 AS STRING) AS mast_account_id,
  CAST(acct.cifp_triad_score_aligned AS STRING) AS cifp_triad_score_aligned,
  CAST(acct.cifp_credit_limit AS STRING) AS cifp_credit_limit,
  CAST(acct.cifp_prev_stat_bal AS STRING) AS cifp_prev_stat_bal,
  FORMAT_DATETIME('%Y-%m-%dT%H:%M:%S', acct.cifp_date_open) AS cifp_date_open,
  FORMAT_DATETIME('%Y-%m-%dT%H:%M:%S', acct.cifp_first_use_date)
    AS cifp_first_use_date,
  acct.cifp_client_product_code,
  CAST(acct.cifp_current_balance AS STRING) AS cifp_current_balance,
  CAST(acct.cifp_amt_pay_ctd AS STRING) AS cifp_amt_pay_ctd,
  CAST(DATE_DIFF(rundate, date(acct.cifp_date_open), month) AS STRING)
    AS open_tob,
  CAST(DATE_DIFF(rundate, date(acct.cifp_first_use_date), month) AS STRING)
    AS tob,
  CAST(
    CASE
      WHEN acct.cifp_prev_stat_bal > 0
        THEN
          round(SAFE_DIVIDE(acct.cifp_amt_pay_ctd, acct.cifp_prev_stat_bal), 2)
      ELSE 9999
      END AS STRING)
    AS pymt_ratio,
  CAST(
    CASE
      WHEN acct.cifp_credit_limit != 0
        THEN
          round(SAFE_DIVIDE(acct.cifp_prev_stat_bal, acct.cifp_credit_limit), 2)
      END AS STRING)
    AS util,
  CAST(acct.CIFP_TEST_DIGIT_TYPE5 AS STRING) AS CIFP_TEST_DIGIT_TYPE5,
  acct.CIFP_Acquisition_code,
  card.CIFP_ZIP_CODE,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_account_management.CIF_ACCOUNT_CURR` acct
INNER JOIN `pcb-{env}-curated.domain_account_management.CIF_CARD_CURR` card
  ON
    acct.cifp_account_id5 = card.cifp_account_id6
    AND IFNULL(card.CIFP_RELATIONSHIP_STAT, '') = ''
    AND card.CIFP_CUSTOMER_TYPE = 0
WHERE
  date(acct.cifp_first_use_date) BETWEEN threshold AND rundate
  AND acct.CIFP_SUFFIX_NUMBER = 0
  AND IFNULL(TRIM(acct.AM00_STATC_CLOSED), '') = ''
  AND IFNULL(TRIM(acct.AM00_STATC_CHARGEOFF), '') = ''
  AND IFNULL(TRIM(acct.AM00_STATC_CREDIT_REVOKED), '') = ''
  AND acct.CIFP_CLIENT_PRODUCT_CODE NOT LIKE "PD%";