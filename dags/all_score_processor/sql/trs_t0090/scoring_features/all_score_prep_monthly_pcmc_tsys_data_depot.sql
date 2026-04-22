-- Pull monthend tsys data from AM0 to populate pcmc data depot
BEGIN
DECLARE report_month DATE;

SET report_month = (
  SELECT (get_report_month).report_month
FROM (SELECT `pcb-{env}-landing.domain_scoring.get_report_month`('{report_year}', '{report_month}') AS get_report_month)
);

  CREATE TEMP TABLE WK_TSYS_AM00 AS
SELECT DISTINCT
  file_create_dt,
  mast_account_id,
  am00_application_suffix,
  am00_billing_cycle AS billing_cyc,
  am00_statc_chargeoff,
  am00_limit_credit AS crln,
  am00_clchg_type AS crln_chg_type,
  EXTRACT(year FROM am00_date_account_open) AS open_yr,
  EXTRACT(month FROM am00_date_account_open) AS open_mth,
  DATE(am00_date_account_open) AS open_dt,
  am00_acq_strategy_code AS acq_stgy_cd,
  CASE
    WHEN NULLIF(am00_statc_current_past_due, '') IS NULL THEN 0
    WHEN am00_statc_current_past_due = '1A' THEN 10
    WHEN am00_statc_current_past_due = '1B' THEN 11
    WHEN am00_statc_current_past_due = '1C' THEN 12
    ELSE SAFE.PARSE_NUMERIC(am00_statc_current_past_due)
END
  AS mthend_pd_stat,
  CASE
    WHEN am00_bankruptcy_type =0 THEN NULL
    ELSE am00_bankruptcy_type
END
  AS bk_type,
  DATE_TRUNC(FILE_CREATE_DT, month) AS process_dt,
  CASE
    WHEN am00_statc_chargeoff IN ('07', '13') THEN 1
END
  AS bk_ind,
  CASE
    WHEN am00_statc_chargeoff IN ('BD', 'ST', 'DC') THEN 1
END
  AS co_ind,
  CASE
    WHEN am00_statc_chargeoff IN ('FR') THEN 1
END
  AS fr_ind,
  CASE
    WHEN (am00_bankruptcy_type <> 0 OR am00_statc_credit_revoked IN ('07', '13')) AND NULLIF(am00_statc_chargeoff, '') IS NULL THEN 1
END
  AS b_ind,
  am00_statc_credit_revoked AS crvk_ind,
  am00_statc_closed AS closed_ind,
  DATE(am00_statc_crvk_mnt_date) AS crvk_dt,
  DATE(am00_statc_closed_date_mnt) AS closed_dt,
  am00_statc_security_fraud AS sec_frd_ind,
  DATE(am00_date_security_fraud_stat) AS sec_frd_dt,
  am00_statf_fraud,
  CASE
    WHEN am00_statf_active_this_month = 'Y' THEN 1
END
  AS sys_active,
  am00_client_product_code,
  am00_disclosure_grp,
  AM00_ALTERNATE_DISCLOSURE_GRP,
  DATE(am00_date_start_alt_discl_grp) AS str_discl_grp,
  DATE(am00_date_stop_alt_discl_grp) AS exp_discl_grp,
  AM00_TRIAD_SCORE_ALIGNED,
  AM00_TRIAD_SCORE_RAW,
  AM00_STATC_DISPUTE,
  AM00_TYPEC_VIP,
  AM00_CUSTOM_DATA_81
FROM
  `pcb-{env}-curated.domain_account_management.AM00`
WHERE
  file_create_dt = report_month
ORDER BY
  mast_account_id,
  am00_application_suffix;

CREATE TEMP TABLE WK_TSYS_AM01 AS
SELECT DISTINCT
  file_create_dt,
  mast_account_id,
  am00_application_suffix,
  am01_account_num AS account_number,
  am01_prim_card_id
FROM
  `pcb-{env}-curated.domain_account_management.AM01`
WHERE
  file_create_dt = report_month
  AND am01_customer_type=0
  AND (am01_account_rel_stat IN ('A')
    OR am01_account_rel_stat IS NULL
    OR am01_account_rel_stat = "")
ORDER BY
  mast_account_id,
  am00_application_suffix;

CREATE TEMP TABLE WK_TSYS_AM02 AS
SELECT DISTINCT
  file_create_dt,
  mast_account_id,
  am00_application_suffix,
  am02_balance_current AS mthend_bal,
  DATE(am02_date_last_purchase) AS lst_mrch_dt,
  DATE(am02_date_last_cash) AS lst_cash_dt,
  DATE(am02_date_last_payment) AS lst_pymt_dt,
  DATE(am02_date_last_credit) AS lst_cred_dt,
  DATE(am02_date_first_use) AS fst_actv_dt
FROM
  `pcb-{env}-curated.domain_account_management.AM02`
WHERE
  file_create_dt = report_month
ORDER BY
  mast_account_id,
  am00_application_suffix;

CREATE TEMP TABLE WK_TSYS_MTHEND_ALL AS
SELECT DISTINCT
  tsys_am00.mast_account_id AS MAST_ACCOUNT_ID,
  tsys_am00.am00_application_suffix AS AM00_APPLICATION_SUFFIX,
  account_number AS ACCOUNT_NUMBER,
  mthend_bal AS MTHEND_BAL,
  billing_cyc AS BILLING_CYC ,
  crln AS CRLN,
  crln_chg_type AS CRLN_CHG_TYPE,
  open_yr AS OPEN_YR,
  open_mth AS OPEN_MTH,
  open_dt AS OPEN_DT,
  acq_stgy_cd AS ACQ_STGY_CD,
  mthend_pd_stat AS MTHEND_PD_STAT,
  bk_type AS BK_TYPE,
  process_dt AS PROCESS_DT,
  bk_ind AS BK_IND,
  co_ind AS CO_IND,
  fr_ind AS FR_IND,
  b_ind AS B_IND,
  lst_mrch_dt AS LST_MRCH_DT,
  lst_cash_dt AS LST_CASH_DT,
  lst_pymt_dt AS LST_PYMT_DT,
  lst_cred_dt AS LST_CRED_DT,
  fst_actv_dt AS FST_ACTV_DT,
  crvk_ind AS CRVK_IND,
  closed_ind AS CLOSED_IND,
  crvk_dt AS CRVK_DT,
  closed_dt AS CLOSED_DT,
  sec_frd_ind AS SEC_FRD_IND,
  sec_frd_dt AS SEC_FRD_DT,
  sys_active AS SYS_ACTIVE,
  am00_client_product_code AS AM00_CLIENT_PRODUCT_CODE,
  am01_prim_card_id AS AM01_PRIM_CARD_ID,
  am00_disclosure_grp AS AM00_DISCLOSURE_GRP ,
  AM00_ALTERNATE_DISCLOSURE_GRP ,
  str_discl_grp AS STR_DISCL_GRP,
  exp_discl_grp AS EXP_DISCL_GRP,
  AM00_TRIAD_SCORE_ALIGNED,
  AM00_TRIAD_SCORE_RAW,
  am00_statf_fraud AS AM00_STATF_FRAUD,
  AM00_STATC_DISPUTE,
  AM00_TYPEC_VIP,
  AM00_CUSTOM_DATA_81,
  CASE
    WHEN crvk_ind IN ('07', '13') AND NULLIF(am00_statc_chargeoff,'') IS NULL AND DATE_TRUNC(crvk_dt, MONTH)=process_dt THEN mthend_bal
END
  AS b_amt,
  CASE
    WHEN crvk_ind IN ('07', '13') AND NULLIF(am00_statc_chargeoff,'') IS NULL AND DATE_TRUNC(crvk_dt, MONTH)=process_dt THEN crvk_dt
END
  AS b_dt
FROM
  WK_TSYS_AM00 tsys_am00
INNER JOIN
  WK_TSYS_AM01 tsys_am01
ON
  tsys_am00.mast_account_id = tsys_am01.mast_account_id
  AND tsys_am00.am00_application_suffix = tsys_am01.am00_application_suffix
  AND tsys_am00.file_create_dt = tsys_am01.file_create_dt
 INNER JOIN
   WK_TSYS_AM02 tsys_am02
 ON
   tsys_am00.mast_account_id = tsys_am02.mast_account_id
   AND tsys_am00.am00_application_suffix = tsys_am02.am00_application_suffix
   AND tsys_am00.file_create_dt = tsys_am02.file_create_dt
WHERE
  am00_client_product_code NOT LIKE 'PD%'
ORDER BY
  tsys_am00.mast_account_id,
  tsys_am00.am00_application_suffix;

CREATE OR REPLACE TABLE
  `pcb-{env}-landing.domain_scoring.WK_TSYS_MTHEND` AS
SELECT DISTINCT
  *
FROM
  WK_TSYS_MTHEND_ALL
WHERE
  AM00_APPLICATION_SUFFIX=0 ;

CREATE OR REPLACE TABLE
  `pcb-{env}-landing.domain_scoring.WK_TSYS_MTHEND_SUFFIX` AS
SELECT DISTINCT
  MAST_ACCOUNT_ID,
  PROCESS_DT,
  SUM(MTHEND_BAL) AS MTHEND_BAL_SUFFIX,
  COUNT(*) AS MTHEND_CNT_SUFFIX
FROM
  WK_TSYS_MTHEND_ALL
WHERE
  AM00_APPLICATION_SUFFIX>0
  AND CO_IND IS NULL
  AND BK_IND IS NULL
  AND FR_IND IS NULL
GROUP BY
  1,
  2
ORDER BY
  1,
  2 ;

CREATE OR REPLACE TABLE
  `pcb-{env}-landing.domain_scoring.WK_MTHEND_ID` AS
SELECT DISTINCT
  MAST_ACCOUNT_ID,
  PARSE_NUMERIC(NULLIF(AM00_CUSTOM_DATA_81,"")) AS APP_NUMBER
FROM
  `pcb-{env}-landing.domain_scoring.WK_TSYS_MTHEND`
WHERE
  open_dt >= '2010-06-26';
END