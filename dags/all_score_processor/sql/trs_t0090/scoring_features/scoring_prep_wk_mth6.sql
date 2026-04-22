BEGIN
  DECLARE report_month DATE;

DECLARE month_list ARRAY<STRING>;
DECLARE month_list_1 STRING;
DECLARE month_list_5 STRING;
DECLARE month_list_9 STRING;
DECLARE month_list_12 STRING;

SET report_month = (
  SELECT (get_report_month).report_month
  FROM
    (
      SELECT
        `pcb-{env}-landing.domain_scoring.get_report_month`(
          '{report_year}', '{report_month}') AS get_report_month
    )
);

SET month_list = (
  SELECT (get_report_month).month_list
  FROM
    (
      SELECT
        `pcb-{env}-landing.domain_scoring.get_report_month`(
          '{report_year}', '{report_month}') AS get_report_month
    )
);

SET month_list_1 = COALESCE(REPLACE(month_list[SAFE_OFFSET(1)], '-', ''), '99991201');
SET month_list_5 = COALESCE(REPLACE(month_list[SAFE_OFFSET(5)], '-', ''), '99991201');
SET month_list_9 = COALESCE(REPLACE(month_list[SAFE_OFFSET(9)], '-', ''), '99991201');
SET month_list_12 = COALESCE(REPLACE(month_list[SAFE_OFFSET(12)], '-', ''), '99991201');

EXECUTE
  IMMEDIATE
    FORMAT(
      """
INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_MTH6`
(
  MAST_ACCOUNT_ID,
  ACCOUNT_NUMBER,
  MTHEND_BAL,
  BILLING_CYC,
  CRLN,
  CRLN_CHG_TYPE,
  OPEN_YR,
  OPEN_MTH,
  OPEN_DT,
  ACQ_STGY_CD,
  MTHEND_PD_STAT,
  BK_TYPE,
  PROCESS_DT,
  BK_IND,
  CO_IND,
  FR_IND,
  B_IND,
  LST_MRCH_DT,
  LST_CASH_DT,
  LST_PYMT_DT,
  LST_CRED_DT,
  FST_ACTV_DT,
  CRVK_IND,
  CLOSED_IND,
  CRVK_DT,
  CLOSED_DT,
  SEC_FRD_IND,
  SEC_FRD_DT,
  SYS_ACTIVE,
  AM00_CLIENT_PRODUCT_CODE,
  AM01_PRIM_CARD_ID,
  AM00_TRIAD_SCORE_ALIGNED,
  AM00_TRIAD_SCORE_RAW,
  AM00_STATF_FRAUD,
  AM00_STATC_DISPUTE,
  AM00_TYPEC_VIP,
  AM00_CUSTOM_DATA_81,
  B_AMT,
  B_DT,
  MTHEND_BAL_SUFFIX,
  MTHEND_CNT_SUFFIX,
  POSTAL_FSA,
  DOB,
  STATE,
  REGION,
  STMT_BAL,
  STMT_CRLN,
  TOT_DUE,
  CYC_PYMT,
  CYC_PYMT_NET,
  ANN_RATE,
  ACT_ANN_RATE,
  PREV_STMT_BAL,
  STMT_DUE_DT,
  STMT_PD_STAT,
  INTERCHG,
  FOREX,
  MTHEND_CO_REC,
  MTHEND_BK_REC,
  MTHEND_FR_REC,
  MTHEND_INT,
  MTHEND_PYMT,
  MTHEND_MRCH,
  MTHEND_CASH,
  MTHEND_TRANS,
  CIBC_CASH_CNT,
  FRGN_CASH_CNT,
  CDN_CASH_CNT,
  OLIM_FEE,
  ABP_FEE,
  CASH_FEE,
  NSF_FEE,
  CRS_FEE,
  IDREST_FEE,
  CREDALRT_FEE,
  RSASST_FEE,
  OTHR_FEE,
  MTHEND_CONVC,
  MTHEND_BTFER,
  ANNUAL_FEE,
  CYC_MRCH,
  CYC_CASH,
  FR_AMT,
  FR_CNT,
  BK_AMT_SUFFIX,
  BK_CNT_SUFFIX,
  CO_AMT_SUFFIX,
  CO_CNT_SUFFIX,
  CHGOFF_DT,
  BK_AMT,
  CO_AMT,
  ORIG_CHGOFF_AMT,
  BNI_SCORE,
  ORIG_BNI_SCORE,
  SCORE_DT,
  CB_SCORE,
  ORIG_CB_SCORE,
  OFFER_DT,
  CURR_LIMIT,
  OFFER_LIMIT,
  BEHV_SCORE_RAW,
  TEST_DIGIT,
  COFF_RSN_CD,
  CO_REC,
  BK_REC,
  STATUS_CD,
  TOB,
  DISCL_GRP,
  OFFER,
  APR_CODE,
  TOB_RANGE,
  CB_SCR_RANGE,
  ACCT_KEY,
  TRANS_IND,
  RVLVR_IND,
  ACTIVE_IND,
  ORIG_CRLN,
  PREV_MTHEND_BAL,
  PREV_MTHEND_BAL_SUFFIX,
  PREV_CB_SCORE_6MTH,
  PREV_CB_SCORE_9MTH,
  PREV_CB_SCORE_12MTH,
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
SELECT DISTINCT
  current_month.* EXCEPT(REC_LOAD_TIMESTAMP, JOB_ID),
  hist_month_list.CRLN AS ORIG_CRLN,
  hist_month_list.MTHEND_BAL AS PREV_MTHEND_BAL,
  hist_month_list.MTHEND_BAL_SUFFIX AS PREV_MTHEND_BAL_SUFFIX,
  hist_month_list.PREV_CB_SCORE_6MTH AS PREV_CB_SCORE_6MTH,
  hist_month_list.PREV_CB_SCORE_9MTH AS PREV_CB_SCORE_9MTH,
  hist_month_list.PREV_CB_SCORE_12MTH AS PREV_CB_SCORE_12MTH,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_scoring.SCORING_PREP_WK_MMM_LATEST` AS current_month
LEFT JOIN
(SELECT DISTINCT * FROM `pcb-{env}-curated.domain_scoring.SCORING_PREP_PCMC_DATA_DEPOT_ALL`
 QUALIFY DENSE_RANK() OVER(PARTITION BY MAST_ACCOUNT_ID, YEAR, MONTH ORDER BY REC_LOAD_TIMESTAMP DESC) = 1
 AND CONCAT(YEAR,MONTH) IN ( SUBSTR('%s', 1, 6), SUBSTR('%s', 1, 6), SUBSTR('%s', 1, 6), SUBSTR('%s', 1, 6))) AS hist_month_list
ON
  CAST(current_month.ACCT_KEY AS STRING) = hist_month_list.ACCT_KEY
  """,
      month_list_1,
      month_list_5,
      month_list_9,
      month_list_12);

END
