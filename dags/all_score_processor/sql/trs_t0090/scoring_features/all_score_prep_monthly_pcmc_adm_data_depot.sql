-- Pull application data from ADM datasets in terminus to populate pcmc data depot
CREATE OR REPLACE TABLE
  `pcb-{env}-landing.domain_scoring.WK_ACE_FINAL` AS
WITH ADM_APP_DATA AS (
SELECT DISTINCT
  CAST(adm_std_app_data.apa_app_num AS integer) AS apa_app_num,
  adm_std_app_data.apa_date_final_disposition,
  CAST(adm_std_app_data.APA_DATE_STATUS_YYYYMMDD AS integer) AS APA_DATE_STATUS_YYYYMMDD,
  CAST(adm_std_app_data.APA_TIME_STATUS_HHMMSS AS integer) AS APA_TIME_STATUS_HHMMSS,
  adm_std_app_data.APA_STATUS,
  adm_std_app_data.APA_QUEUE_ID,
  adm_std_app_data.APA_ASSIGNED_CREDIT_LIMIT,
  CAST(adm_std_app_data.APA_DATE_ENTERED_YYYYMMDD AS integer) AS APA_DATE_ENTERED_YYYYMMDD,
  adm_std_app_data.apa_test_account_flag,
  CAST(adm_std_app_data.EXECUTION_ID AS integer) AS execution_id,
  adm_std_app_data.APA_STRATEGY,
  adm_std_tmpl_info.DTC_ACCOUNT_CUSTOM_DATA_40,
  adm_std_tmpl_info.DTC_PROMO_SOLICITATION_CODE,
  adm_std_tmpl_info.DTC_ACQUISITION_STRATEGY_CODE,
  CAST(adm_std_addr_info.dtc_Sequence AS integer) AS Address_Sequence,
  adm_std_addr_info.DTC_STATE
FROM
  `pcb-{env}-curated.domain_customer_acquisition.ADM_STD_DISP_APP_DATA` adm_std_app_data
LEFT JOIN
  `pcb-{env}-curated.domain_customer_acquisition.ADM_STD_DISP_TS2T_TMPL_INFO` adm_std_tmpl_info
ON
  adm_std_app_data.apa_app_num=adm_std_tmpl_info.apa_app_num
  AND adm_std_app_data.EXECUTION_ID=adm_std_tmpl_info.EXECUTION_ID
LEFT JOIN (
  SELECT DISTINCT
    apa_app_num,
    dtc_Sequence,
    DTC_STATE,
    EXECUTION_ID
  FROM
    `pcb-{env}-curated.domain_customer_acquisition.ADM_STD_DISP_ADDR_INFO`
  WHERE
    DTC_sequence IN (1)) adm_std_addr_info
ON
  adm_std_app_data.apa_app_num=adm_std_addr_info.apa_app_num
  AND adm_std_app_data.EXECUTION_ID=adm_std_addr_info.EXECUTION_ID
WHERE
  adm_std_app_data.APA_DATE_ENTERED_YYYYMMDD >= '20150329'
  AND adm_std_app_data.EXECUTION_ID > 0
  AND NULLIF(apa_test_account_flag, "") IS NULL
  AND adm_std_app_data.APA_STRATEGY IN ('PCBSTRAT')
ORDER BY
  apa_app_num,
  APA_DATE_STATUS_YYYYMMDD DESC,
  APA_TIME_STATUS_HHMMSS DESC,
  EXECUTION_ID
),

ADM_DE_DATA AS (
SELECT
  CAST(adm_disp_app_data.apa_app_num AS integer) AS apa_app_num,
  adm_disp_app_data.apa_test_account_flag,
  CAST(adm_disp_app_data.EXECUTION_ID AS integer) AS EXECUTION_ID,
  adm_disp_app_data.APA_STRATEGY,
  adm_disp_dt_elmt.ele_element_name,
  adm_disp_dt_elmt.ELE_ELEMENT_TYPE,
  adm_disp_dt_elmt.ELE_ALPHA_VALUE,
  CAST(adm_disp_dt_elmt.ELE_NUMERIC_VALUE AS integer) AS ELE_NUMERIC_VALUE,
  REPLACE(TRIM(adm_disp_dt_elmt.ele_element_name), ' ', '_') AS ele_element_name_clean,
  CAST(adm_disp_dt_elmt.ELE_NUMERIC_VALUE AS FLOAT64) / 100000.0 AS ELE_NUMERIC_VALUE2
FROM
  `pcb-{env}-curated.domain_customer_acquisition.ADM_STD_DISP_APP_DATA` adm_disp_app_data
LEFT JOIN
  `pcb-{env}-curated.domain_customer_acquisition.ADM_STD_DISP_DT_ELMT` adm_disp_dt_elmt
ON
  adm_disp_app_data.apa_app_num = adm_disp_dt_elmt.apa_app_num
  AND adm_disp_app_data.EXECUTION_ID=adm_disp_dt_elmt.EXECUTION_ID
WHERE
  adm_disp_app_data.EXECUTION_ID > 0
  AND adm_disp_app_data.APA_DATE_ENTERED_YYYYMMDD >= '20150329'
  AND adm_disp_app_data.APA_STRATEGY IN ('PCBSTRAT')
  AND adm_disp_dt_elmt.ele_element_name IN ( 'BUREAU_SCORE',
    'BUREAU_SELECTED',
    'SCORECARD_SELECTED',
    'SCORECARD_ID',
    'APP_SCORE',
    'BNI_SCORE' )
ORDER BY
  adm_disp_app_data.apa_app_num,
  adm_disp_dt_elmt.ele_element_name,
  adm_disp_app_data.EXECUTION_ID
),

ADM_DE_NUMERIC AS (
SELECT DISTINCT *
FROM (
  SELECT DISTINCT apa_app_num, EXECUTION_ID, ELE_NUMERIC_VALUE2, ele_element_name_clean 
  FROM ADM_DE_DATA 
  WHERE ELE_ELEMENT_TYPE = 'N'
)
PIVOT (
  ANY_VALUE(ELE_NUMERIC_VALUE2) FOR ele_element_name_clean IN (
    'BUREAU_SCORE',
    'APP_SCORE',
    'BNI_SCORE'
  )
)
ORDER BY
  apa_app_num,
  EXECUTION_ID
),

ADM_DE_ALPHA AS (
SELECT DISTINCT *
FROM (
  SELECT DISTINCT apa_app_num, EXECUTION_ID, ELE_ALPHA_VALUE, ele_element_name_clean 
  FROM ADM_DE_DATA 
  WHERE ELE_ELEMENT_TYPE = 'A'
)
PIVOT (
  ANY_VALUE(ELE_ALPHA_VALUE) FOR ele_element_name_clean IN (
    'BUREAU_SELECTED',
    'SCORECARD_SELECTED',
    'SCORECARD_ID'
  )
)
ORDER BY
  apa_app_num,
  EXECUTION_ID
),

ADM_JOINED AS (
SELECT DISTINCT
  adm_data.*,
  bureau_selected AS cb_type,
  bureau_score AS orgn_cb_score,
  bni_score AS orgn_bni_score,
  app_score,
  scorecard_id,
  scorecard_selected
FROM
  ADM_APP_DATA adm_data
LEFT JOIN
  ADM_DE_NUMERIC adm_numeric
ON
  adm_data.apa_app_num=adm_numeric.apa_app_num
  AND adm_data.EXECUTION_ID=adm_numeric.EXECUTION_ID
LEFT JOIN
  ADM_DE_ALPHA adm_alpha
ON
  adm_data.apa_app_num=adm_alpha.apa_app_num
  AND adm_data.EXECUTION_ID=adm_alpha.EXECUTION_ID
WHERE
  adm_data.apa_status NOT IN ('VOID',
    'VOID APP')
  AND adm_data.apa_app_num NOT IN (
  SELECT
    DISTINCT apa_app_num
  FROM
    ADM_APP_DATA
  WHERE
    apa_status IN ('VOID',
      'VOID APP'))
ORDER BY
  adm_data.apa_app_num,
  adm_data.EXECUTION_ID
)
SELECT DISTINCT
  apa_app_num AS APP_NUMBER,
  apa_assigned_credit_limit AS CREDIT_LIMIT_GRANTED,
  PARSE_DATE('%Y%m%d',NULLIF(apa_date_final_disposition,"")) AS DATE_FINAL_DISPOSITION,
  PARSE_DATE('%Y%m%d',SAFE_CAST(apa_date_entered_yyyymmdd AS string)) AS APP_ENTERED_DATE,
  PARSE_DATE('%Y%m%d',SAFE_CAST(apa_date_status_yyyymmdd AS string)) AS APP_MODIFIED_DATE,
  apa_time_status_hhmmss AS APP_MODIFIED_TIME,
  CASE
    WHEN apa_status = 'NEWACCOUNT' THEN 'APPROVED'
    WHEN apa_status = 'DECLINE'
  AND apa_queue_id != 'AMLVERID' THEN 'DECLINED'
    WHEN apa_status IN ('WITHDRAW', 'TRUE DUPL', 'NORESPONSE') THEN 'DECLINED'
    ELSE 'PENDING'
END
  AS APP_STATUS,
  APA_STRATEGY,
  cb_type AS CB_TYPE,
  orgn_cb_score AS ORGN_CB_SCORE,
  orgn_bni_score AS ORGN_BNI_SCORE,
  app_score AS APP_SCORE,
  scorecard_id AS SCORECARD_ID,
  scorecard_selected AS SCORECARD_SELECTED,
  dtc_account_custom_data_40 AS P_FI_CE_EN_EMP_STAT,
  dtc_acquisition_strategy_code AS DTC_ACQUISITION_STRATEGY_CODE,
  dtc_promo_solicitation_code AS DTC_PROMO_SOLICITATION_CODE,
  DTC_STATE AS P_PI_CA_EN_PROV,
  SAFE_CAST(dtc_acquisition_strategy_code AS string) AS APP_SOURCE_CODE,
  SAFE_CAST(dtc_promo_solicitation_code AS integer) AS PROMO_CODE,
  -999 AS TOT_NUM_TRADES,
  -999 AS CLORTRADES,
  -999 AS MAJ_NUM_MDR,
  -999 AS TU_P_ESG_NUM_MTH_FLE,
  -999 AS TU_P_ESG_NUM_REV_TR,
  -999 AS CC_PO_B_PT_LNID,
  -999 AS P_FI_MTHLY_INCOME,
  app_score AS CC_PO_B_SC_RSK_S,
  CASE
    WHEN scorecard_selected = 'CLEAN AND THICK' THEN 3
    WHEN scorecard_selected = 'DIRTY OR THIN' THEN 4
    WHEN scorecard_selected = 'NO HIT' THEN 7
    WHEN scorecard_selected = 'NO SCORE' THEN 8
    ELSE -999
END
  AS CC_PO_B_SC_RSK_SCID,
FROM (
  SELECT DISTINCT
    *,
    ROW_NUMBER() OVER (PARTITION BY apa_app_num ORDER BY APA_DATE_STATUS_YYYYMMDD DESC, APA_TIME_STATUS_HHMMSS DESC, COALESCE(orgn_cb_score,app_score) DESC,
      COALESCE(DTC_ACCOUNT_CUSTOM_DATA_40,dtc_promo_solicitation_code) DESC,
      COALESCE(DTC_STATE) DESC) AS rn
  FROM
    ADM_JOINED )
WHERE
  rn = 1
ORDER BY
  apa_app_num;