BEGIN

DECLARE report_month DATE;
DECLARE month_list ARRAY<STRING>;
DECLARE month_list_0 STRING;
DECLARE month_list_1 STRING;

SET report_month = (
  SELECT (get_report_month).report_month
FROM (SELECT `pcb-{env}-landing.domain_scoring.get_report_month`('{report_year}', '{report_month}') AS get_report_month)
);

SET month_list = (
  SELECT (get_report_month).month_list
FROM (SELECT `pcb-{env}-landing.domain_scoring.get_report_month`('{report_year}', '{report_month}') AS get_report_month)
);

SET month_list_0 = REPLACE(month_list[SAFE_OFFSET(0)],'-','');
SET month_list_1 = REPLACE(month_list[SAFE_OFFSET(1)],'-','');


EXECUTE IMMEDIATE FORMAT("""
CREATE TEMP TABLE WK_MMM_1 AS
        SELECT DISTINCT
            wk_tsys_mthend.*, 
            wk_tsys_mthend_suffix.* EXCEPT(MAST_ACCOUNT_ID, PROCESS_DT),
            wk_tsys_demgfx.* EXCEPT(MAST_ACCOUNT_ID, PROCESS_DT, AM00_APPLICATION_SUFFIX),
            wk_tsys_cycle.* EXCEPT(MAST_ACCOUNT_ID, PROCESS_DT),
            wk_tsys_trans.* EXCEPT(MAST_ACCOUNT_ID, PROCESS_DT),
            wk_tsys_cyc_trans.* EXCEPT(MAST_ACCOUNT_ID, PROCESS_DT),
            wk_tsys_chgoff_fr.* EXCEPT(MAST_ACCOUNT_ID, PROCESS_DT),
            wk_tsys_chgoff_suffix.* EXCEPT(MAST_ACCOUNT_ID, PROCESS_DT),
            wk_tsys_chgoff.* EXCEPT(MAST_ACCOUNT_ID, PROCESS_DT, FR_AMT, AM00_APPLICATION_SUFFIX ),
            wk_tsys_bni_score.* EXCEPT(MAST_ACCOUNT_ID, PROCESS_DT, SCORE_TYPE, SCORE_DT),
            wk_tsys_cb_score.* EXCEPT(MAST_ACCOUNT_ID, PROCESS_DT, SCORE_TYPE),
            wk_cli_offer_data.* EXCEPT(MAST_ACCOUNT_ID, PROCESS_DT),
            wk_raw_score.* EXCEPT(MAST_ACCOUNT_ID, PROCESS_DT, SCORE_DT),
            wk_test_digit_month_list.* EXCEPT(MAST_ACCOUNT_ID, PROCESS_DT, SPID, FILE_CREATE_DT),


        FROM `pcb-{env}-landing.domain_scoring.WK_TSYS_MTHEND` AS wk_tsys_mthend
        
        LEFT JOIN `pcb-{env}-landing.domain_scoring.WK_TSYS_MTHEND_SUFFIX` AS wk_tsys_mthend_suffix
        ON wk_tsys_mthend.MAST_ACCOUNT_ID = wk_tsys_mthend_suffix.MAST_ACCOUNT_ID AND wk_tsys_mthend.PROCESS_DT = wk_tsys_mthend_suffix.PROCESS_DT
        
        LEFT JOIN `pcb-{env}-landing.domain_scoring.WK_TSYS_DEMGFX` AS wk_tsys_demgfx
        ON wk_tsys_mthend.MAST_ACCOUNT_ID = wk_tsys_demgfx.MAST_ACCOUNT_ID AND wk_tsys_mthend.PROCESS_DT = wk_tsys_demgfx.PROCESS_DT

        LEFT JOIN `pcb-{env}-landing.domain_scoring.WK_TSYS_CYCLE` AS wk_tsys_cycle
        ON wk_tsys_mthend.MAST_ACCOUNT_ID = wk_tsys_cycle.MAST_ACCOUNT_ID AND wk_tsys_mthend.PROCESS_DT = wk_tsys_cycle.PROCESS_DT

        LEFT JOIN `pcb-{env}-landing.domain_scoring.WK_TSYS_TRANS` AS wk_tsys_trans
        ON wk_tsys_mthend.MAST_ACCOUNT_ID = wk_tsys_trans.MAST_ACCOUNT_ID AND wk_tsys_mthend.PROCESS_DT = wk_tsys_trans.PROCESS_DT
        
        LEFT JOIN `pcb-{env}-landing.domain_scoring.WK_TSYS_CYC_TRANS` AS wk_tsys_cyc_trans
        ON wk_tsys_mthend.MAST_ACCOUNT_ID = wk_tsys_cyc_trans.MAST_ACCOUNT_ID AND wk_tsys_mthend.PROCESS_DT = wk_tsys_cyc_trans.PROCESS_DT
        
        LEFT JOIN `pcb-{env}-landing.domain_scoring.WK_TSYS_CHGOFF_FR` AS wk_tsys_chgoff_fr
        ON wk_tsys_mthend.MAST_ACCOUNT_ID = wk_tsys_chgoff_fr.MAST_ACCOUNT_ID AND wk_tsys_mthend.PROCESS_DT = wk_tsys_chgoff_fr.PROCESS_DT
        
        LEFT JOIN `pcb-{env}-landing.domain_scoring.WK_TSYS_CHGOFF_SUFFIX` AS wk_tsys_chgoff_suffix
        ON wk_tsys_mthend.MAST_ACCOUNT_ID = wk_tsys_chgoff_suffix.MAST_ACCOUNT_ID AND wk_tsys_mthend.PROCESS_DT = wk_tsys_chgoff_suffix.PROCESS_DT
        
        LEFT JOIN `pcb-{env}-landing.domain_scoring.WK_TSYS_CHGOFF` AS wk_tsys_chgoff
        ON wk_tsys_mthend.MAST_ACCOUNT_ID = wk_tsys_chgoff.MAST_ACCOUNT_ID AND wk_tsys_mthend.PROCESS_DT = wk_tsys_chgoff.PROCESS_DT
        
        LEFT JOIN `pcb-{env}-landing.domain_scoring.WK_TSYS_BNI_SCORE` AS wk_tsys_bni_score
        ON CAST(wk_tsys_mthend.MAST_ACCOUNT_ID AS STRING) = wk_tsys_bni_score.MAST_ACCOUNT_ID AND wk_tsys_mthend.PROCESS_DT = wk_tsys_bni_score.PROCESS_DT
        
        LEFT JOIN `pcb-{env}-landing.domain_scoring.WK_TSYS_CB_SCORE` AS wk_tsys_cb_score
        ON CAST(wk_tsys_mthend.MAST_ACCOUNT_ID AS STRING) = wk_tsys_cb_score.MAST_ACCOUNT_ID AND wk_tsys_mthend.PROCESS_DT = wk_tsys_cb_score.PROCESS_DT
        
        LEFT JOIN `pcb-{env}-landing.domain_scoring.WK_CLI_OFFER_DATA` AS wk_cli_offer_data
        ON wk_tsys_mthend.MAST_ACCOUNT_ID = wk_cli_offer_data.MAST_ACCOUNT_ID AND wk_tsys_mthend.PROCESS_DT = wk_cli_offer_data.PROCESS_DT
        
        LEFT JOIN `pcb-{env}-landing.domain_scoring.WK_RAW_SCORE` AS wk_raw_score
        ON wk_tsys_mthend.MAST_ACCOUNT_ID = wk_raw_score.MAST_ACCOUNT_ID AND wk_tsys_mthend.PROCESS_DT = wk_raw_score.PROCESS_DT
        
        LEFT JOIN `pcb-{env}-landing.domain_scoring.WK_TEST_DIGIT_%s` AS wk_test_digit_month_list
        ON wk_tsys_mthend.MAST_ACCOUNT_ID = wk_test_digit_month_list.MAST_ACCOUNT_ID
        
    """, month_list_0);
    
    CREATE TEMP TABLE WK_MMM_2 AS
        SELECT DISTINCT
            wk_mmm_1.* ,
            wk_mmm_1.AM00_STATC_CHARGEOFF AS COFF_RSN_CD, 

            CASE WHEN wk_mmm_1.CO_IND=1 THEN wk_mmm_1.CYC_PYMT END AS CO_REC,
            CASE WHEN wk_mmm_1.BK_IND=1 THEN wk_mmm_1.CYC_PYMT END AS BK_REC,

            CASE 
              WHEN (wk_mmm_1.CO_IND IS NOT NULL OR wk_mmm_1.BK_IND IS NOT NULL  OR wk_mmm_1.FR_IND IS NOT NULL ) THEN 'Z'
              WHEN wk_mmm_1.B_IND IS NOT NULL THEN 'B'
              WHEN wk_mmm_1.CRVK_IND != "" THEN 'E'
              WHEN wk_mmm_1.SEC_FRD_IND != "" THEN 'U'
              WHEN wk_mmm_1.CLOSED_IND != "" THEN 'C'
            END AS STATUS_CD,

            DATE_DIFF(wk_mmm_1.PROCESS_DT, wk_mmm_1.OPEN_DT, MONTH ) AS TOB,

            CASE 
              WHEN wk_mmm_1.PROCESS_DT>=wk_mmm_1.STR_DISCL_GRP AND wk_mmm_1.PROCESS_DT<wk_mmm_1.EXP_DISCL_GRP THEN wk_mmm_1.AM00_ALTERNATE_DISCLOSURE_GRP 
              ELSE wk_mmm_1.AM00_DISCLOSURE_GRP 
              END AS DISCL_GRP,

        FROM WK_MMM_1 AS wk_mmm_1;

    CREATE OR REPLACE TEMP TABLE WK_MMM_3 AS
      SELECT DISTINCT
        wk_mmm_2.* EXCEPT(AM00_DISCLOSURE_GRP, AM00_ALTERNATE_DISCLOSURE_GRP,STR_DISCL_GRP, 
            EXP_DISCL_GRP, AM00_APPLICATION_SUFFIX, AM00_STATC_CHARGEOFF, AH01_CDATE_CYCLE, RN, OFFER),
        
        CASE WHEN wk_mmm_2.OFFER IS NULL THEN 0 ELSE wk_mmm_2.OFFER END AS OFFER,

        CASE 
          WHEN wk_mmm_2.DISCL_GRP IN ('QCAPR001','QCAPR002', 'QCAPR003', 'QCAPR004','RCAPR001','RCAPR002', 'RCAPR003', 'RCAPR004') THEN 'PROMO'
          WHEN wk_mmm_2.DISCL_GRP IN ('QCSTD001','RCSTD001') THEN 'STANDARD'
          WHEN wk_mmm_2.DISCL_GRP IN ('QCDFT001','RCDFT001') THEN 'DEFAULT'
          WHEN wk_mmm_2.DISCL_GRP IN ('QCPRF001','RCPRF001') THEN 'PERFORM'
          WHEN wk_mmm_2.DISCL_GRP IN ('QCCCS001', 'QCHRD001', 'RCCCS001', 'RCHRD001') THEN 'COLLECT'
          END AS APR_CODE,

        CASE
          WHEN wk_mmm_2.TOB < 6  THEN 5  
          WHEN wk_mmm_2.TOB < 12 THEN 11 
          WHEN wk_mmm_2.TOB < 18   THEN 17 
          WHEN wk_mmm_2.TOB < 24   THEN 23 
          WHEN wk_mmm_2.TOB >=24   THEN 24     
          END AS TOB_RANGE,

        CASE
          WHEN wk_mmm_2.CB_SCORE IS NULL THEN NULL 
          WHEN CAST(wk_mmm_2.CB_SCORE AS FLOAT64)= 0 THEN 0 
          WHEN CAST(wk_mmm_2.CB_SCORE AS FLOAT64) < 620 THEN 619 
          WHEN CAST(wk_mmm_2.CB_SCORE AS FLOAT64) < 660 THEN 659 
          WHEN CAST(wk_mmm_2.CB_SCORE AS FLOAT64) < 740 THEN 739 
          WHEN CAST(wk_mmm_2.CB_SCORE AS FLOAT64) < 800 THEN 799 
          WHEN CAST(wk_mmm_2.CB_SCORE AS FLOAT64) >=800 THEN 800   
          END AS CB_SCR_RANGE,

        -- ACCT_KEY IN PAST DD TABLES IS INT64 SO SKIP SAS FORMAT FOR NOW 
        -- RPAD(CAST(MAST_ACCOUNT_ID AS STRING), 16, " ") AS ACCT_KEY,
        wk_mmm_2.MAST_ACCOUNT_ID AS ACCT_KEY,
        
        CASE
            WHEN wk_mmm_2.CYC_PYMT = 0 THEN NULL   
            WHEN wk_mmm_2.CYC_PYMT >= wk_mmm_2.PREV_STMT_BAL OR (wk_mmm_2.CYC_PYMT IS NULL AND wk_mmm_2.PREV_STMT_BAL IS NULL) THEN 1  -- ADDED NULL CONDITIONS FOR NEW ACCOUNTS WHICH CATEGORIZED AS TRANSACTOR AS DEFAULT 
            END AS TRANS_IND, 
        CASE
            WHEN wk_mmm_2.CYC_PYMT = 0 THEN NULL   
            WHEN wk_mmm_2.CYC_PYMT <  PREV_STMT_BAL THEN 1  
          END AS RVLVR_IND,
          

     FROM WK_MMM_2 AS wk_mmm_2
     ORDER BY wk_mmm_2.MAST_ACCOUNT_ID;

    -- SEPARATE ACTIVE INDICATOR CONDITION TO A SEPARATE STEP FOR EASIER MODIFICATION IN THE FUTURE

EXECUTE IMMEDIATE FORMAT("""
    CREATE OR REPLACE TABLE `pcb-{env}-landing.domain_scoring.WK_MMM` AS
      SELECT DISTINCT 
        wk_mmm_3.*,
        CASE
          --/*ALL ACCOUNTS*/
          WHEN 
            (wk_mmm_3.MTHEND_BAL <> 0 OR wk_mmm_3.LST_MRCH_DT >= wk_mmm_3.PROCESS_DT OR wk_mmm_3.LST_CASH_DT >= wk_mmm_3.PROCESS_DT OR 
            wk_mmm_3.LST_PYMT_DT >= wk_mmm_3.PROCESS_DT OR wk_mmm_3.LST_CRED_DT >= wk_mmm_3.PROCESS_DT) 
            THEN 1
          END AS ACTIVE_IND
    FROM WK_MMM_3 AS wk_mmm_3
    LEFT JOIN `pcb-{env}-landing.domain_scoring.SCORING_PREP_PCMC_DATA_DEPOT` AS hist_data_depot    
    ON CAST(wk_mmm_3.ACCT_KEY AS STRING) = hist_data_depot.ACCT_KEY
    AND CONCAT(hist_data_depot.YEAR,hist_data_depot.MONTH) = SUBSTR('%s', 1, 6)
      """, month_list_1);
END
