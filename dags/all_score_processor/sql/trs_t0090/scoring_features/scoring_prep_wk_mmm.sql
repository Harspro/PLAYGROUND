BEGIN
  DECLARE report_month DATE;

DECLARE month_list ARRAY<STRING>;
DECLARE month_list_0 STRING;
DECLARE month_list_1 STRING;

SET report_month = (
  SELECT (get_report_month).report_month
  FROM
    (
      SELECT
        `pcb-{env}-landing.domain_scoring.get_report_month`('{report_year}', '{report_month}')
          AS get_report_month
    )
);

SET month_list = (
  SELECT (get_report_month).month_list
  FROM
    (
      SELECT
        `pcb-{env}-landing.domain_scoring.get_report_month`('{report_year}', '{report_month}')
          AS get_report_month
    )
);

SET month_list_0 = COALESCE(REPLACE(month_list[SAFE_OFFSET(0)], '-', ''), '99991201');
SET month_list_1 = COALESCE(REPLACE(month_list[SAFE_OFFSET(1)], '-', ''), '99991201');

EXECUTE
  IMMEDIATE
    FORMAT(
      """
-- base table MTHEND

INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_MMM`
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
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
WITH base_data AS (
  SELECT DISTINCT wk_tsys_mthend_latest.* EXCEPT(REC_LOAD_TIMESTAMP, JOB_ID), 
         wk_tsys_mthend_suffix_latest.* EXCEPT(MAST_ACCOUNT_ID, PROCESS_DT, REC_LOAD_TIMESTAMP, JOB_ID)
  FROM `pcb-{env}-curated.domain_scoring.SCORING_PREP_WK_TSYS_MTHEND_FULL_LATEST` as wk_tsys_mthend_latest
  LEFT JOIN `pcb-{env}-curated.domain_scoring.SCORING_PREP_WK_TSYS_MTHEND_SUFFIX_LATEST` AS wk_tsys_mthend_suffix_latest
  ON wk_tsys_mthend_latest.MAST_ACCOUNT_ID = wk_tsys_mthend_suffix_latest.MAST_ACCOUNT_ID 
  AND wk_tsys_mthend_latest.PROCESS_DT = wk_tsys_mthend_suffix_latest.PROCESS_DT
  WHERE CAST(wk_tsys_mthend_latest.AM00_APPLICATION_SUFFIX AS INT64)=0
),

-- Add demographic,cycle,transaction data
enriched_step1 AS (
SELECT DISTINCT
        base_data.*,
        wk_tsys_demgfx_latest.* EXCEPT(MAST_ACCOUNT_ID, PROCESS_DT, AM00_APPLICATION_SUFFIX, REC_LOAD_TIMESTAMP, JOB_ID),
        wk_tsys_cycle_latest.* EXCEPT(MAST_ACCOUNT_ID, PROCESS_DT, REC_LOAD_TIMESTAMP, JOB_ID),
        wk_tsys_trans_latest.* EXCEPT(MAST_ACCOUNT_ID, PROCESS_DT, REC_LOAD_TIMESTAMP, JOB_ID),
        wk_tsys_cycle_trans_latest.* EXCEPT(MAST_ACCOUNT_ID, PROCESS_DT, REC_LOAD_TIMESTAMP, JOB_ID),
        wk_test_digit_latest.* EXCEPT(MAST_ACCOUNT_ID, PROCESS_DT, SPID, FILE_CREATE_DT,REPORT_DATE,REC_LOAD_TIMESTAMP, JOB_ID)
FROM
      base_data 
      LEFT JOIN `pcb-{env}-curated.domain_scoring.SCORING_PREP_WK_TSYS_DEMGFX_LATEST` AS wk_tsys_demgfx_latest
      ON  base_data.MAST_ACCOUNT_ID = wk_tsys_demgfx_latest.MAST_ACCOUNT_ID 
      AND base_data.PROCESS_DT = wk_tsys_demgfx_latest.PROCESS_DT

      LEFT JOIN (select *, ROW_NUMBER() OVER (PARTITION BY MAST_ACCOUNT_ID ORDER BY AH01_CDATE_CYCLE) AS RN
                  from `pcb-{env}-curated.domain_scoring.SCORING_PREP_WK_TSYS_CYCLE_LATEST`
                 ) AS wk_tsys_cycle_latest
      ON base_data.MAST_ACCOUNT_ID = wk_tsys_cycle_latest.MAST_ACCOUNT_ID 
      AND base_data.PROCESS_DT = wk_tsys_cycle_latest.PROCESS_DT
      AND wk_tsys_cycle_latest.RN =1

      LEFT JOIN `pcb-{env}-curated.domain_scoring.SCORING_PREP_WK_TSYS_TRANS_LATEST` AS wk_tsys_trans_latest
      ON base_data.MAST_ACCOUNT_ID = wk_tsys_trans_latest.MAST_ACCOUNT_ID 
      AND base_data.PROCESS_DT = wk_tsys_trans_latest.PROCESS_DT
        
      LEFT JOIN `pcb-{env}-curated.domain_scoring.SCORING_PREP_WK_TSYS_CYCLE_TRANS_LATEST` AS wk_tsys_cycle_trans_latest
      ON base_data.MAST_ACCOUNT_ID = wk_tsys_cycle_trans_latest.MAST_ACCOUNT_ID 
      AND base_data.PROCESS_DT = wk_tsys_cycle_trans_latest.PROCESS_DT

      LEFT JOIN `pcb-{env}-curated.domain_scoring.SCORING_PREP_WK_TEST_DIGIT_LATEST` AS wk_test_digit_latest
      ON base_data.MAST_ACCOUNT_ID = wk_test_digit_latest.MAST_ACCOUNT_ID
      AND REPLACE(wk_test_digit_latest.REPORT_DATE,'-','') = '%s'
),

-- Add charge-off, scores, CLI data
enriched_step2 AS (
SELECT DISTINCT
            enriched_step1.* EXCEPT (RN),
            wk_tsys_chgoff_full_latest.* EXCEPT(MAST_ACCOUNT_ID, PROCESS_DT, FR_AMT, AM00_APPLICATION_SUFFIX, REC_LOAD_TIMESTAMP, JOB_ID ),
            wk_tsys_chgoff_fr_latest.* EXCEPT(MAST_ACCOUNT_ID, PROCESS_DT,REC_LOAD_TIMESTAMP, JOB_ID),
            wk_tsys_chgoff_suffix_latest.* EXCEPT(MAST_ACCOUNT_ID, PROCESS_DT,REC_LOAD_TIMESTAMP, JOB_ID),
            wk_tsys_bni_score_latest.* EXCEPT(MAST_ACCOUNT_ID, PROCESS_DT, SCORE_TYPE, SCORE_DT,REC_LOAD_TIMESTAMP, JOB_ID),
            wk_tsys_cb_score_latest.* EXCEPT(MAST_ACCOUNT_ID, PROCESS_DT, SCORE_TYPE,REC_LOAD_TIMESTAMP, JOB_ID),
            wk_cli_offer_data_latest.* EXCEPT(MAST_ACCOUNT_ID, PROCESS_DT,REC_LOAD_TIMESTAMP, JOB_ID),
            wk_raw_score_latest.* EXCEPT(MAST_ACCOUNT_ID, PROCESS_DT, SCORE_DT,REC_LOAD_TIMESTAMP, JOB_ID),

FROM enriched_step1

        LEFT JOIN `pcb-{env}-curated.domain_scoring.SCORING_PREP_WK_TSYS_CHGOFF_FULL_LATEST` AS wk_tsys_chgoff_full_latest
        ON enriched_step1.MAST_ACCOUNT_ID = wk_tsys_chgoff_full_latest.MAST_ACCOUNT_ID 
        AND enriched_step1.PROCESS_DT = wk_tsys_chgoff_full_latest.PROCESS_DT
        AND CAST(wk_tsys_chgoff_full_latest.AM00_APPLICATION_SUFFIX AS INT64)=0
        
        LEFT JOIN `pcb-{env}-curated.domain_scoring.SCORING_PREP_WK_TSYS_CHGOFF_FR_LATEST` AS wk_tsys_chgoff_fr_latest
        ON enriched_step1.MAST_ACCOUNT_ID = wk_tsys_chgoff_fr_latest.MAST_ACCOUNT_ID 
        AND enriched_step1.PROCESS_DT = wk_tsys_chgoff_fr_latest.PROCESS_DT
        
        LEFT JOIN `pcb-{env}-curated.domain_scoring.SCORING_PREP_WK_TSYS_CHGOFF_SUFFIX_LATEST` AS wk_tsys_chgoff_suffix_latest
        ON enriched_step1.MAST_ACCOUNT_ID = wk_tsys_chgoff_suffix_latest.MAST_ACCOUNT_ID 
        AND enriched_step1.PROCESS_DT = wk_tsys_chgoff_suffix_latest.PROCESS_DT
        
        LEFT JOIN `pcb-{env}-curated.domain_scoring.SCORING_PREP_WK_TSYS_BNI_SCORE_LATEST` AS wk_tsys_bni_score_latest
        ON enriched_step1.MAST_ACCOUNT_ID = wk_tsys_bni_score_latest.MAST_ACCOUNT_ID 
        AND enriched_step1.PROCESS_DT = wk_tsys_bni_score_latest.PROCESS_DT
        
        LEFT JOIN `pcb-{env}-curated.domain_scoring.SCORING_PREP_WK_TSYS_CB_SCORE_LATEST` AS wk_tsys_cb_score_latest
        ON enriched_step1.MAST_ACCOUNT_ID = wk_tsys_cb_score_latest.MAST_ACCOUNT_ID 
        AND enriched_step1.PROCESS_DT = wk_tsys_cb_score_latest.PROCESS_DT
        
        LEFT JOIN `pcb-{env}-curated.domain_scoring.SCORING_PREP_WK_CLI_OFFER_DATA_LATEST` AS wk_cli_offer_data_latest
        ON enriched_step1.MAST_ACCOUNT_ID = wk_cli_offer_data_latest.MAST_ACCOUNT_ID 
        AND enriched_step1.PROCESS_DT = wk_cli_offer_data_latest.PROCESS_DT
        
        LEFT JOIN `pcb-{env}-curated.domain_scoring.SCORING_PREP_WK_RAW_SCORE_LATEST` AS wk_raw_score_latest
        ON enriched_step1.MAST_ACCOUNT_ID = wk_raw_score_latest.MAST_ACCOUNT_ID 
        AND enriched_step1.PROCESS_DT = wk_raw_score_latest.PROCESS_DT

),
enriched_step3 AS (
    
    SELECT DISTINCT
            enriched_step2.* ,
            enriched_step2.AM00_STATC_CHARGEOFF AS COFF_RSN_CD, 

            CASE WHEN enriched_step2.CO_IND='1' THEN enriched_step2.CYC_PYMT END AS CO_REC,
            CASE WHEN enriched_step2.BK_IND='1' THEN enriched_step2.CYC_PYMT END AS BK_REC,

            CASE 
              WHEN (enriched_step2.CO_IND IS NOT NULL OR enriched_step2.BK_IND IS NOT NULL  OR enriched_step2.FR_IND IS NOT NULL ) THEN 'Z'
              WHEN enriched_step2.B_IND IS NOT NULL THEN 'B'
              WHEN enriched_step2.CRVK_IND != "" THEN 'E'
              WHEN enriched_step2.SEC_FRD_IND != "" THEN 'U'
              WHEN enriched_step2.CLOSED_IND != "" THEN 'C'
            END AS STATUS_CD,

            CAST(DATE_DIFF(CAST(enriched_step2.PROCESS_DT AS DATE), CAST(enriched_step2.OPEN_DT AS DATE), MONTH ) AS STRING) AS TOB,
      
            CASE 
              WHEN CAST(enriched_step2.PROCESS_DT AS DATE) >= CAST(enriched_step2.STR_DISCL_GRP AS DATE) 
              AND  CAST(enriched_step2.PROCESS_DT AS DATE) <  CAST(enriched_step2.EXP_DISCL_GRP AS DATE) 
              THEN enriched_step2.AM00_ALTERNATE_DISCLOSURE_GRP 
              ELSE enriched_step2.AM00_DISCLOSURE_GRP 
              END AS DISCL_GRP,
            
            DATE_DIFF(CAST(enriched_step2.PROCESS_DT AS DATE), CAST(enriched_step2.OPEN_DT AS DATE), MONTH ) AS TOB_INT,
            CAST(enriched_step2.CB_SCORE AS FLOAT64) AS CB_SCORE_FLOAT

        FROM enriched_step2 AS enriched_step2
),       
enriched_step4 AS (

  SELECT DISTINCT
        enriched_step3.* EXCEPT(AM00_DISCLOSURE_GRP, AM00_ALTERNATE_DISCLOSURE_GRP,STR_DISCL_GRP, 
            EXP_DISCL_GRP, AM00_APPLICATION_SUFFIX, AM00_STATC_CHARGEOFF, AH01_CDATE_CYCLE, OFFER, TOB_INT, CB_SCORE_FLOAT),
        
        CASE WHEN enriched_step3.OFFER IS NULL THEN '0' ELSE enriched_step3.OFFER END AS OFFER,

        CASE 
          WHEN enriched_step3.DISCL_GRP IN ('QCAPR001','QCAPR002', 'QCAPR003', 'QCAPR004','RCAPR001','RCAPR002', 'RCAPR003', 'RCAPR004') THEN 'PROMO'
          WHEN enriched_step3.DISCL_GRP IN ('QCSTD001','RCSTD001') THEN 'STANDARD'
          WHEN enriched_step3.DISCL_GRP IN ('QCDFT001','RCDFT001') THEN 'DEFAULT'
          WHEN enriched_step3.DISCL_GRP IN ('QCPRF001','RCPRF001') THEN 'PERFORM'
          WHEN enriched_step3.DISCL_GRP IN ('QCCCS001', 'QCHRD001', 'RCCCS001', 'RCHRD001') THEN 'COLLECT'
          END AS APR_CODE,

        CAST(CASE
          WHEN enriched_step3.TOB_INT < 6  THEN 5  
          WHEN enriched_step3.TOB_INT < 12 THEN 11 
          WHEN enriched_step3.TOB_INT < 18 THEN 17 
          WHEN enriched_step3.TOB_INT < 24 THEN 23
          WHEN enriched_step3.TOB_INT >=24 THEN 24     
          END AS STRING) AS TOB_RANGE,

        CAST(CASE
          WHEN enriched_step3.CB_SCORE IS NULL THEN NULL 
          WHEN enriched_step3.CB_SCORE_FLOAT = 0 THEN 0 
          WHEN enriched_step3.CB_SCORE_FLOAT < 620 THEN 619 
          WHEN enriched_step3.CB_SCORE_FLOAT < 660 THEN 659 
          WHEN enriched_step3.CB_SCORE_FLOAT < 740 THEN 739 
          WHEN enriched_step3.CB_SCORE_FLOAT < 800 THEN 799 
          WHEN enriched_step3.CB_SCORE_FLOAT >=800 THEN 800   
          END AS STRING) AS CB_SCR_RANGE,

        -- ACCT_KEY IN PAST DD TABLES IS INT64 SO SKIP SAS FORMAT FOR NOW 
        -- RPAD(CAST(MAST_ACCOUNT_ID AS STRING), 16, " ") AS ACCT_KEY,
        enriched_step3.MAST_ACCOUNT_ID AS ACCT_KEY,
        
        CAST(CASE
            WHEN enriched_step3.CYC_PYMT = '0' THEN NULL   
            WHEN enriched_step3.CYC_PYMT >= enriched_step3.PREV_STMT_BAL 
                 OR (enriched_step3.CYC_PYMT IS NULL 
                 AND enriched_step3.PREV_STMT_BAL IS NULL) THEN '1'  -- ADDED NULL CONDITIONS FOR NEW ACCOUNTS WHICH CATEGORIZED AS TRANSACTOR AS DEFAULT 
            END AS STRING) AS TRANS_IND, 
        CAST(CASE
            WHEN enriched_step3.CYC_PYMT = '0' THEN NULL   
            WHEN enriched_step3.CYC_PYMT <  PREV_STMT_BAL THEN '1'  
          END AS STRING) AS RVLVR_IND,
          

     FROM enriched_step3 AS enriched_step3
)

SELECT DISTINCT 
        enriched_step4.MAST_ACCOUNT_ID,
        enriched_step4.ACCOUNT_NUMBER,
        enriched_step4.MTHEND_BAL,
        enriched_step4.BILLING_CYC,
        enriched_step4.CRLN,
        enriched_step4.CRLN_CHG_TYPE,
        enriched_step4.OPEN_YR,
        enriched_step4.OPEN_MTH,
        enriched_step4.OPEN_DT,
        enriched_step4.ACQ_STGY_CD,
        enriched_step4.MTHEND_PD_STAT,
        enriched_step4.BK_TYPE,
        enriched_step4.PROCESS_DT,
        enriched_step4.BK_IND,
        enriched_step4.CO_IND,
        enriched_step4.FR_IND,
        enriched_step4.B_IND,
        enriched_step4.LST_MRCH_DT,
        enriched_step4.LST_CASH_DT,
        enriched_step4.LST_PYMT_DT,
        enriched_step4.LST_CRED_DT,
        enriched_step4.FST_ACTV_DT,
        enriched_step4.CRVK_IND,
        enriched_step4.CLOSED_IND,
        enriched_step4.CRVK_DT,
        enriched_step4.CLOSED_DT,
        enriched_step4.SEC_FRD_IND,
        enriched_step4.SEC_FRD_DT,
        enriched_step4.SYS_ACTIVE,
        enriched_step4.AM00_CLIENT_PRODUCT_CODE,
        enriched_step4.AM01_PRIM_CARD_ID,
        enriched_step4.AM00_TRIAD_SCORE_ALIGNED,
        enriched_step4.AM00_TRIAD_SCORE_RAW,
        enriched_step4.AM00_STATF_FRAUD,
        enriched_step4.AM00_STATC_DISPUTE,
        enriched_step4.AM00_TYPEC_VIP,
        enriched_step4.AM00_CUSTOM_DATA_81,
        enriched_step4.B_AMT,
        enriched_step4.B_DT,
        enriched_step4.MTHEND_BAL_SUFFIX,
        enriched_step4.MTHEND_CNT_SUFFIX,
        enriched_step4.POSTAL_FSA,
        enriched_step4.DOB,
        enriched_step4.STATE,
        enriched_step4.REGION,
        enriched_step4.STMT_BAL,
        enriched_step4.STMT_CRLN,
        enriched_step4.TOT_DUE,
        enriched_step4.CYC_PYMT,
        enriched_step4.CYC_PYMT_NET,
        enriched_step4.ANN_RATE,
        enriched_step4.ACT_ANN_RATE,
        enriched_step4.PREV_STMT_BAL,
        enriched_step4.STMT_DUE_DT,
        enriched_step4.STMT_PD_STAT,
        enriched_step4.INTERCHG,
        enriched_step4.FOREX,
        enriched_step4.MTHEND_CO_REC,
        enriched_step4.MTHEND_BK_REC,
        enriched_step4.MTHEND_FR_REC,
        enriched_step4.MTHEND_INT,
        enriched_step4.MTHEND_PYMT,
        enriched_step4.MTHEND_MRCH,
        enriched_step4.MTHEND_CASH,
        enriched_step4.MTHEND_TRANS,
        enriched_step4.CIBC_CASH_CNT,
        enriched_step4.FRGN_CASH_CNT,
        enriched_step4.CDN_CASH_CNT,
        enriched_step4.OLIM_FEE,
        enriched_step4.ABP_FEE,
        enriched_step4.CASH_FEE,
        enriched_step4.NSF_FEE,
        enriched_step4.CRS_FEE,
        enriched_step4.IDREST_FEE,
        enriched_step4.CREDALRT_FEE,
        enriched_step4.RSASST_FEE,
        enriched_step4.OTHR_FEE,
        enriched_step4.MTHEND_CONVC,
        enriched_step4.MTHEND_BTFER,
        enriched_step4.ANNUAL_FEE,
        enriched_step4.CYC_MRCH,
        enriched_step4.CYC_CASH,
        enriched_step4.FR_AMT,
        enriched_step4.FR_CNT,
        enriched_step4.BK_AMT_SUFFIX,
        enriched_step4.BK_CNT_SUFFIX,
        enriched_step4.CO_AMT_SUFFIX,
        enriched_step4.CO_CNT_SUFFIX,
        enriched_step4.CHGOFF_DT,
        enriched_step4.BK_AMT,
        enriched_step4.CO_AMT,
        enriched_step4.ORIG_CHGOFF_AMT,
        enriched_step4.BNI_SCORE,
        enriched_step4.ORIG_BNI_SCORE,
        enriched_step4.SCORE_DT,
        enriched_step4.CB_SCORE,
        enriched_step4.ORIG_CB_SCORE,
        enriched_step4.OFFER_DT,
        enriched_step4.CURR_LIMIT,
        enriched_step4.OFFER_LIMIT,
        enriched_step4.BEHV_SCORE_RAW,
        enriched_step4.TEST_DIGIT,
        enriched_step4.COFF_RSN_CD,
        enriched_step4.CO_REC,
        enriched_step4.BK_REC,
        enriched_step4.STATUS_CD,
        enriched_step4.TOB,
        enriched_step4.DISCL_GRP,
        enriched_step4.OFFER,
        enriched_step4.APR_CODE,
        enriched_step4.TOB_RANGE,
        enriched_step4.CB_SCR_RANGE,
        enriched_step4.ACCT_KEY,
        enriched_step4.TRANS_IND,
        enriched_step4.RVLVR_IND,

        CAST(CASE
          --/*ALL ACCOUNTS*/
          WHEN 
            (enriched_step4.MTHEND_BAL <> '0' OR enriched_step4.LST_MRCH_DT >= enriched_step4.PROCESS_DT OR enriched_step4.LST_CASH_DT >= enriched_step4.PROCESS_DT OR 
            enriched_step4.LST_PYMT_DT >= enriched_step4.PROCESS_DT OR enriched_step4.LST_CRED_DT >= enriched_step4.PROCESS_DT) 
            THEN '1'
          END AS STRING) AS ACTIVE_IND,
          CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
          '{dag_id}' AS JOB_ID
    FROM enriched_step4 AS enriched_step4
    LEFT JOIN (SELECT DISTINCT * FROM `pcb-{env}-curated.domain_scoring.SCORING_PREP_PCMC_DATA_DEPOT_ALL`  
               QUALIFY DENSE_RANK() OVER(PARTITION BY MAST_ACCOUNT_ID, YEAR, MONTH ORDER BY REC_LOAD_TIMESTAMP DESC) = 1 
               AND CONCAT(YEAR,MONTH) = SUBSTR('%s', 1, 6)) AS hist_data_depot
    ON CAST(enriched_step4.ACCT_KEY AS STRING) = hist_data_depot.ACCT_KEY

""",
      month_list_0,
      month_list_1);

END
