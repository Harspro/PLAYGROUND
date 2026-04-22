-- Pull Experian Accounts to populate pcmc data depot
CREATE OR REPLACE TABLE `pcb-{env}-landing.domain_scoring.WK_ACQUISITION` AS
WITH MTH_ACQUISITION AS (
  SELECT DISTINCT
            CAST(APP_NUMBER AS string) AS APP_NUMBER,
            CAST(CREDIT_LIMIT_GRANTED AS string) AS CREDIT_LIMIT_GRANTED,
            CAST(app_entered_date AS string) AS app_entered_date,
            CAST(app_modified_date AS string) AS app_modified_date,
            CAST(APP_MODIFIED_TIME AS string)AS APP_MODIFIED_TIME,
            CAST(NULL AS STRING) AS APP_STATUS_BEFORE,
            CAST(NULL AS STRING) AS APP_STATUS_AFTER,
            APP_SOURCE_CODE,
            APP_STATUS,
            CAST(CC_PO_B_PT_LNID AS string) AS CC_PO_B_PT_LNID,
            CAST(CC_PO_B_SC_RSK_S AS string) AS CC_PO_B_SC_RSK_S,
            CAST(CC_PO_B_SC_RSK_SCID AS string) AS CC_PO_B_SC_RSK_SCID,
            CAST(P_FI_CE_EN_EMP_STAT AS string) AS P_FI_CE_EN_EMP_STAT,
            CAST(P_FI_MTHLY_INCOME AS string) AS P_FI_MTHLY_INCOME,
            P_PI_CA_EN_PROV,
            CAST(PROMO_CODE  AS string) AS PROMO_CODE,
            CAST(TU_P_ESG_NUM_MTH_FLE  AS string) AS TU_P_ESG_NUM_MTH_FLE,
            CAST(TU_P_ESG_NUM_REV_TR AS string) AS TU_P_ESG_NUM_REV_TR,
            CAST(orgn_cb_score  AS string) AS orgn_cb_score,
            CAST(orgn_bni_score AS string) AS orgn_bni_score,
            CAST(NULL AS STRING) AS ORIGINAL_ACCOUNT,
            CAST(NULL AS STRING) AS CC_CV_C_DD_PBPS,
            CAST(NULL AS STRING) AS EQ_P_ESG_BEACON_SCR,
            CAST(NULL AS STRING) AS EQ_S_ESG_BEACON_SCR,
            CAST(NULL AS STRING) AS TU_P_ESG_EMPERICA_SCR,
            CAST(NULL AS STRING) AS TU_S_ESG_EMPERICA_SCR,
            CAST(NULL AS STRING) AS EQ_P_ESG_BNK_NAV_INDEX,
            CAST(NULL AS STRING) AS EQ_S_ESG_BNK_NAV_INDEX,
            CAST(NULL AS STRING) AS TU_P_ESG_HOR_BNKRT_SCR,
            CAST(NULL AS STRING) AS TU_S_ESG_HOR_BNKRT_SCR,
            CAST(NULL AS STRING) AS EQ_P_ESG_TOT_NUM_TR,
            CAST(NULL AS STRING) AS EQ_S_ESG_TOT_NUM_TR,
            CAST(NULL AS STRING) AS TU_P_ESG_TOT_NUM_TR,
            CAST(NULL AS STRING) AS TU_S_ESG_TOT_NUM_TR,
            CAST(NULL AS STRING) AS EQ_P_ESG_CLORTRADES,
            CAST(NULL AS STRING) AS EQ_S_ESG_CLORTRADES,
            CAST(NULL AS STRING) AS TU_P_ESG_CLORTRADES,
            CAST(NULL AS STRING) AS TU_S_ESG_CLORTRADES,
            CAST(NULL AS STRING) AS EQ_P_PCG_MAJ_NUM_MDR,
            CAST(NULL AS STRING) AS EQ_S_PCG_MAJ_NUM_MDR,
            CAST(NULL AS STRING) AS TU_P_PCG_MAJ_NUM_MDR,
            CAST(NULL AS STRING) AS TU_S_PCG_MAJ_NUM_MDR,
            CAST(NULL AS STRING) AS CC_SYSTEM_RCCC1,
            CAST(NULL AS STRING) AS P_FI_PCFCUST_IND,
            CAST(NULL AS STRING) AS P_FI_HOUSING_STATUS_E,
            CAST(NULL AS STRING) AS STORE,
            CAST(NULL AS STRING) AS IMAGE_ID,
            CAST(NULL AS STRING) AS CC_PO_B_PT_RN,
            CAST(NULL AS STRING) AS CC_PO_B_SC_RSK_SCN,
            APA_STRATEGY,
            CAST(APP_SCORE AS string) AS APP_SCORE,
            SCORECARD_ID,
            DTC_ACQUISITION_STRATEGY_CODE,
            DTC_PROMO_SOLICITATION_CODE,
            CAST(date_final_disposition AS string) AS date_final_disposition
        FROM `pcb-{env}-landing.domain_scoring.WK_ACE_FINAL`
        UNION ALL
        SELECT DISTINCT
            APP_NUMBER,
            CREDIT_LIMIT_GRANTED,
            FORMAT_DATE('%Y-%m-%d',parse_date("%d%b%y", app_entered_date))  AS app_entered_date,
            FORMAT_DATE('%Y-%m-%d',parse_date("%d%b%y", app_modified_date)) AS app_modified_date,
            APP_MODIFIED_TIME,
            APP_STATUS_BEFORE,
            APP_STATUS_AFTER,
            APP_SOURCE_CODE,
            APP_STATUS,
            CC_PO_B_PT_LNID,
            CC_PO_B_SC_RSK_S,
            CC_PO_B_SC_RSK_SCID,
            P_FI_CE_EN_EMP_STAT,
            P_FI_MTHLY_INCOME,
            P_PI_CA_EN_PROV,
            PROMO_CODE,
            TU_P_ESG_NUM_MTH_FLE,
            TU_P_ESG_NUM_REV_TR,
            CAST(NULL AS STRING) AS orgn_cb_score,
            CAST(NULL AS STRING) AS orgn_bni_score,
            ORIGINAL_ACCOUNT,
            CC_CV_C_DD_PBPS,
            EQ_P_ESG_BEACON_SCR,
            EQ_S_ESG_BEACON_SCR,
            TU_P_ESG_EMPERICA_SCR,
            TU_S_ESG_EMPERICA_SCR,
            EQ_P_ESG_BNK_NAV_INDEX,
            EQ_S_ESG_BNK_NAV_INDEX,
            TU_P_ESG_HOR_BNKRT_SCR,
            TU_S_ESG_HOR_BNKRT_SCR,
            EQ_P_ESG_TOT_NUM_TR,
            EQ_S_ESG_TOT_NUM_TR,
            TU_P_ESG_TOT_NUM_TR,
            TU_S_ESG_TOT_NUM_TR,
            EQ_P_ESG_CLORTRADES,
            EQ_S_ESG_CLORTRADES,
            TU_P_ESG_CLORTRADES,
            TU_S_ESG_CLORTRADES,
            EQ_P_PCG_MAJ_NUM_MDR,
            EQ_S_PCG_MAJ_NUM_MDR,
            TU_P_PCG_MAJ_NUM_MDR,
            TU_S_PCG_MAJ_NUM_MDR,
            CC_SYSTEM_RCCC1,
            P_FI_PCFCUST_IND,
            P_FI_HOUSING_STATUS_E,
            STORE,
            IMAGE_ID,
            CC_PO_B_PT_RN,
            CC_PO_B_SC_RSK_SCN,
            CAST(NULL AS STRING) AS APA_STRATEGY,
            CAST(NULL AS STRING) AS APP_SCORE,
            CAST(NULL AS STRING) AS SCORECARD_ID,
            CAST(NULL AS STRING) AS DTC_ACQUISITION_STRATEGY_CODE,
            CAST(NULL AS STRING) AS DTC_PROMO_SOLICITATION_CODE,
            CAST(NULL AS STRING) AS date_final_disposition
        FROM `pcb-{env}-landing.domain_scoring.SCORING_PREP_MTH_ACQUISITION_EQ`
),
WK_ACQUISITION_BASE AS (
        SELECT DISTINCT *,
          original_account as orgn_acct,
          FROM (
              SELECT DISTINCT *, orgn_cb_score AS orgn_cb_score2,orgn_bni_score AS orgn_bni_score2,
                      ROW_NUMBER() OVER (PARTITION BY APP_NUMBER ORDER BY app_modified_date DESC, APP_MODIFIED_TIME DESC) AS rn
              FROM MTH_ACQUISITION
             WHERE app_modified_date < '2015-03-29' 
              AND app_modified_date >= '2010-06-26'
          )
          WHERE rn = 1
          ORDER BY APP_NUMBER, app_modified_date DESC, APP_MODIFIED_TIME DESC
)
SELECT DISTINCT
    APP_NUMBER,
    CREDIT_LIMIT_GRANTED,
    app_entered_date,
    app_modified_date,
    APP_MODIFIED_TIME,
    APP_STATUS_BEFORE,
    APP_STATUS_AFTER,
    APP_SOURCE_CODE,
    APP_STATUS,
    CC_PO_B_PT_LNID,
    CC_PO_B_SC_RSK_S,
    CC_PO_B_SC_RSK_SCID,
    P_FI_CE_EN_EMP_STAT,
    P_FI_MTHLY_INCOME,
    P_PI_CA_EN_PROV,
    PROMO_CODE,
    TU_P_ESG_NUM_MTH_FLE,
    TU_P_ESG_NUM_REV_TR,
    CASE
      WHEN cc_cv_c_dd_pbps IN ('1 EQ','2 EQ','1 TU','2 TU','3 EQ','4 EQ','3 TU','4 TU') THEN
        CASE
          WHEN cc_cv_c_dd_pbps IN ('1 EQ','2 EQ') THEN CAST(eq_p_esg_beacon_scr AS STRING)
          WHEN cc_cv_c_dd_pbps IN ('1 TU','2 TU') THEN CAST(tu_p_esg_emperica_scr AS STRING)
          WHEN cc_cv_c_dd_pbps IN ('3 EQ','4 EQ') THEN CAST(eq_s_esg_beacon_scr AS STRING)
          WHEN cc_cv_c_dd_pbps IN ('3 TU','4 TU') THEN CAST(tu_s_esg_emperica_scr AS STRING)
        END
      WHEN orgn_cb_score IS null THEN '-1'
      ELSE CAST(orgn_cb_score AS STRING)
    END AS ORGN_CB_SCORE,
    CASE
      WHEN cc_cv_c_dd_pbps IN ('1 EQ','2 EQ','1 TU','2 TU','3 EQ','4 EQ','3 TU','4 TU') THEN
        CASE
          WHEN cc_cv_c_dd_pbps IN ('1 EQ','2 EQ') THEN CAST(eq_p_esg_bnk_nav_index AS STRING)
          WHEN cc_cv_c_dd_pbps IN ('1 TU','2 TU') THEN CAST(tu_p_esg_hor_bnkrt_scr AS STRING)
          WHEN cc_cv_c_dd_pbps IN ('3 EQ','4 EQ') THEN CAST(eq_s_esg_bnk_nav_index AS STRING)
          WHEN cc_cv_c_dd_pbps IN ('3 TU','4 TU') THEN CAST(tu_s_esg_hor_bnkrt_scr AS STRING)
        END
      ELSE CAST(orgn_bni_score AS STRING)
    END AS ORGN_BNI_SCORE,
    ORIGINAL_ACCOUNT,
    CC_CV_C_DD_PBPS,
    EQ_P_ESG_BEACON_SCR,
    EQ_S_ESG_BEACON_SCR,
    TU_P_ESG_EMPERICA_SCR,
    TU_S_ESG_EMPERICA_SCR,
    EQ_P_ESG_BNK_NAV_INDEX,
    EQ_S_ESG_BNK_NAV_INDEX,
    TU_P_ESG_HOR_BNKRT_SCR,
    TU_S_ESG_HOR_BNKRT_SCR,
    EQ_P_ESG_TOT_NUM_TR,
    EQ_S_ESG_TOT_NUM_TR,
    TU_P_ESG_TOT_NUM_TR,
    TU_S_ESG_TOT_NUM_TR,
    EQ_P_ESG_CLORTRADES,
    EQ_S_ESG_CLORTRADES,
    TU_P_ESG_CLORTRADES,
    TU_S_ESG_CLORTRADES,
    EQ_P_PCG_MAJ_NUM_MDR,
    EQ_S_PCG_MAJ_NUM_MDR,
    TU_P_PCG_MAJ_NUM_MDR,
    TU_S_PCG_MAJ_NUM_MDR,
    CC_SYSTEM_RCCC1,
    P_FI_PCFCUST_IND,
    P_FI_HOUSING_STATUS_E,
    STORE,
    IMAGE_ID,
    CC_PO_B_PT_RN,
    CC_PO_B_SC_RSK_SCN,
    APA_STRATEGY,
    APP_SCORE,
    SCORECARD_ID,
    DTC_ACQUISITION_STRATEGY_CODE,
    DTC_PROMO_SOLICITATION_CODE,
    DATE_FINAL_DISPOSITION,
    ORGN_CB_SCORE2,
    ORGN_BNI_SCORE2,
    RN,
    ORGN_ACCT,
    CASE
      WHEN cc_cv_c_dd_pbps IN ('1 EQ','2 EQ') THEN CAST(eq_p_esg_tot_num_tr AS STRING)
      WHEN cc_cv_c_dd_pbps IN ('1 TU','2 TU') THEN CAST(tu_p_esg_tot_num_tr AS STRING)
      WHEN cc_cv_c_dd_pbps IN ('3 EQ','4 EQ') THEN CAST(eq_s_esg_tot_num_tr AS STRING)
      WHEN cc_cv_c_dd_pbps IN ('3 TU','4 TU') THEN CAST(tu_s_esg_tot_num_tr AS STRING)
    END AS TOT_NUM_TRADES,
    CASE
      WHEN cc_cv_c_dd_pbps IN ('1 EQ','2 EQ') THEN CAST(eq_p_esg_clortrades AS STRING)
      WHEN cc_cv_c_dd_pbps IN ('1 TU','2 TU') THEN CAST(tu_p_esg_clortrades AS STRING)
      WHEN cc_cv_c_dd_pbps IN ('3 EQ','4 EQ') THEN CAST(eq_s_esg_clortrades AS STRING)
      WHEN cc_cv_c_dd_pbps IN ('3 TU','4 TU') THEN CAST(tu_s_esg_clortrades AS STRING)
    END AS CLORTRADES,
    CASE
      WHEN cc_cv_c_dd_pbps IN ('1 EQ','2 EQ') THEN CAST(eq_p_pcg_maj_num_mdr AS STRING)
      WHEN cc_cv_c_dd_pbps IN ('1 TU','2 TU') THEN CAST(tu_p_pcg_maj_num_mdr AS STRING)
      WHEN cc_cv_c_dd_pbps IN ('3 EQ','4 EQ') THEN CAST(eq_s_pcg_maj_num_mdr AS STRING)
      WHEN cc_cv_c_dd_pbps IN ('3 TU','4 TU') THEN CAST(tu_s_pcg_maj_num_mdr AS STRING)
    END AS MAJ_NUM_MDR,
    CASE
      WHEN cc_cv_c_dd_pbps IN ('1 EQ','2 EQ','3 EQ','4 EQ') THEN 'EQ'
      WHEN cc_cv_c_dd_pbps IN ('1 TU','2 TU','3 TU','4 TU') THEN 'TU'
    END AS CB_TYPE
FROM WK_ACQUISITION_BASE;
