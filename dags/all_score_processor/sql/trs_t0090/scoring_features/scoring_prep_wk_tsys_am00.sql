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

INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_TSYS_AM00`
(
  FILE_CREATE_DT,
  MAST_ACCOUNT_ID,
  AM00_APPLICATION_SUFFIX,
  BILLING_CYC,
  AM00_STATC_CHARGEOFF,
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
  CRVK_IND,
  CLOSED_IND,
  CRVK_DT,
  CLOSED_DT,
  SEC_FRD_IND,
  SEC_FRD_DT,
  AM00_STATF_FRAUD,
  SYS_ACTIVE,
  AM00_CLIENT_PRODUCT_CODE,
  AM00_DISCLOSURE_GRP,
  AM00_ALTERNATE_DISCLOSURE_GRP,
  STR_DISCL_GRP,
  EXP_DISCL_GRP,
  AM00_TRIAD_SCORE_ALIGNED,
  AM00_TRIAD_SCORE_RAW,
  AM00_STATC_DISPUTE,
  AM00_TYPEC_VIP,
  AM00_CUSTOM_DATA_81,
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
SELECT DISTINCT
  CAST(file_create_dt AS STRING) AS FILE_CREATE_DT,
  CAST(mast_account_id AS STRING) AS MAST_ACCOUNT_ID,
  CAST(am00_application_suffix AS STRING) AS AM00_APPLICATION_SUFFIX,
  CAST(am00_billing_cycle AS STRING) AS BILLING_CYC,
  CAST(am00_statc_chargeoff AS STRING) AS AM00_STATC_CHARGEOFF,
  CAST(am00_limit_credit AS STRING) AS CRLN,
  CAST(am00_clchg_type AS STRING) AS CRLN_CHG_TYPE,
  CAST(EXTRACT(year FROM am00_date_account_open) AS STRING) AS OPEN_YR,
  CAST(EXTRACT(month FROM am00_date_account_open) AS STRING) AS OPEN_MTH,
  CAST(DATE(am00_date_account_open) AS STRING) AS OPEN_DT,
  CAST(am00_acq_strategy_code AS STRING) AS ACQ_STGY_CD,
  CAST(
    CASE
      WHEN NULLIF(am00_statc_current_past_due, '') IS NULL THEN 0
      WHEN am00_statc_current_past_due = '1A' THEN 10
      WHEN am00_statc_current_past_due = '1B' THEN 11
      WHEN am00_statc_current_past_due = '1C' THEN 12
      ELSE SAFE.PARSE_NUMERIC(am00_statc_current_past_due)
      END
      AS STRING) AS MTHEND_PD_STAT,
  CAST(
    CASE
      WHEN am00_bankruptcy_type = 0 THEN NULL
      ELSE am00_bankruptcy_type
      END
      AS STRING) AS BK_TYPE,
  CAST(DATE_TRUNC(FILE_CREATE_DT, month) AS STRING) AS PROCESS_DT,
  CAST(
    CASE
      WHEN am00_statc_chargeoff IN ('07', '13') THEN 1
      END
      AS STRING) AS BK_IND,
  CAST(
    CASE
      WHEN am00_statc_chargeoff IN ('BD', 'ST', 'DC') THEN 1
      END
      AS STRING) AS CO_IND,
  CAST(
    CASE
      WHEN am00_statc_chargeoff IN ('FR') THEN 1
      END
      AS STRING) AS FR_IND,
  CAST(
    CASE
      WHEN
        (am00_bankruptcy_type <> 0 OR am00_statc_credit_revoked IN ('07', '13'))
        AND NULLIF(am00_statc_chargeoff, '') IS NULL
        THEN 1
      END
      AS STRING) AS B_IND,
  CAST(am00_statc_credit_revoked AS STRING) AS CRVK_IND,
  CAST(am00_statc_closed AS STRING) AS CLOSED_IND,
  CAST(DATE(am00_statc_crvk_mnt_date) AS STRING) AS CRVK_DT,
  CAST(DATE(am00_statc_closed_date_mnt) AS STRING) AS CLOSED_DT,
  CAST(am00_statc_security_fraud AS STRING) AS SEC_FRD_IND,
  CAST(DATE(am00_date_security_fraud_stat) AS STRING) AS SEC_FRD_DT,
  CAST(am00_statf_fraud AS STRING) AS AM00_STATF_FRAUD,
  CAST(
    CASE
      WHEN am00_statf_active_this_month = 'Y' THEN 1
      END
      AS STRING) AS SYS_ACTIVE,
  CAST(am00_client_product_code AS STRING) AS AM00_CLIENT_PRODUCT_CODE,
  CAST(am00_disclosure_grp AS STRING) AS AM00_DISCLOSURE_GRP,
  CAST(AM00_ALTERNATE_DISCLOSURE_GRP AS STRING)
    AS AM00_ALTERNATE_DISCLOSURE_GRP,
  CAST(DATE(am00_date_start_alt_discl_grp) AS STRING) AS STR_DISCL_GRP,
  CAST(DATE(am00_date_stop_alt_discl_grp) AS STRING) AS EXP_DISCL_GRP,
  CAST(AM00_TRIAD_SCORE_ALIGNED AS STRING) AS AM00_TRIAD_SCORE_ALIGNED,
  CAST(AM00_TRIAD_SCORE_RAW AS STRING) AS AM00_TRIAD_SCORE_RAW,
  CAST(AM00_STATC_DISPUTE AS STRING) AS AM00_STATC_DISPUTE,
  CAST(AM00_TYPEC_VIP AS STRING) AS AM00_TYPEC_VIP,
  CAST(AM00_CUSTOM_DATA_81 AS STRING) AS AM00_CUSTOM_DATA_81,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_account_management.AM00`
WHERE
  file_create_dt = report_month;

END
