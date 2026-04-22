INSERT INTO
    `pcb-{env}-landing.domain_scoring.INPUT_INDEX4_T0040_FEATURES` (
        FEATURES,
        REC_LOAD_TIMESTAMP,
        JOB_ID
    ) WITH -- ============================================================================
    -- STEP 1: Read scoring prep tables
    -- ============================================================================
    das_historical AS (
        SELECT DISTINCT
            *
        FROM
            `pcb-{env}-curated.domain_scoring.SCORING_PREP_CREDSCORE_DAS_HISTORICAL_LATEST`
    ),
    cif_snapshot AS (
        SELECT DISTINCT
            MAST_ACCOUNT_ID,
            CIFP_ZIP_CODE
        FROM
            `pcb-{env}-curated.domain_scoring.SCORING_PREP_CIF_SNAPSHOT_LATEST`
    ),
    existing_pcmc_pcma AS (
        SELECT DISTINCT
            MAST_ACCOUNT_ID,
            wallet_id
        FROM
            `pcb-{env}-curated.domain_scoring.SCORING_PREP_EXISTING_PCMC_PCMA_LATEST`
    ),
    pre_app_score AS (
        SELECT DISTINCT
            wallet_id,
            p6_tot_dollar_collect,
            p6_txns_credit
        FROM
            `pcb-{env}-curated.domain_scoring.SCORING_PREP_PRE_APP_SCORE_LATEST`
    ),
    pco_score AS (
        SELECT DISTINCT
            wallet_id,
            v1_mch0_salesshr_cat5_6m
        FROM
            `pcb-{env}-curated.domain_scoring.SCORING_PREP_PCO_SCORE_LATEST`
    ),
    transaction_summary AS (
        SELECT DISTINCT
            mast_account_id,
            total_dist_mccs,
            tot_sales,
            tot_txns,
            merchant_risk_cat_1_trans_amt,
            num_trans_risk_cat_1
        FROM
            `pcb-{env}-curated.domain_scoring.SCORING_PREP_TRANSACTION_SUMMARY_DATAMART_LATEST`
    ),
    area_risk_lookup AS (
        SELECT DISTINCT
            AREA_CD,
            AREA_RANK
        FROM
            `pcb-{env}-curated.domain_scoring.SCORING_PREP_AREA_RISK_LOOKUP_LATEST`
    ),
    -- ============================================================================
    -- STEP 2: Compute 02_CRI_DATAMART time-series aggregations with joins
    -- ============================================================================
    -- This CTE:
    -- 1. Computes time-series aggregations from DAS_HISTORICAL
    -- 2. Joins to all scoring_prep tables to populate PMML inputs
    -- 3. Computes derived fields (product_hierarchy_rank, prop_clean_live)
    -- ============================================================================
    -- ============================================================================
    -- STEP 2: Compute 02_CRI_DATAMART time-series aggregations with joins
    -- ============================================================================
    -- This CTE:
    -- 1. Computes time-series aggregations from DAS_HISTORICAL
    -- 2. Joins to all scoring_prep tables to populate PMML inputs
    -- 3. Computes derived fields (product_hierarchy_rank, prop_clean_live)
    -- ============================================================================
    base_joined_data AS (
        SELECT DISTINCT
            d.MAST_ACCOUNT_ID,
            -- Fields for deduplication (if multiple wallets per account, keep one with lowest p6_tot_sales)
            CAST(
                COALESCE(pco.v1_mch0_salesshr_cat5_6m, '0') AS NUMERIC
            ) AS p6_tot_sales_numeric,
            -- ========================================================================
            -- Segmentation Variables
            -- ========================================================================
            d.NUM_DEROG_PUB_t0,
            d.AM_NUM_PD_t0 AS CURR_PD_t0,
            d.BC62_t0,
            -- ========================================================================
            -- Fields needed for product_hierarchy_rank and prop_clean_live
            -- ========================================================================
            d.MG_OP_NUM_t0,
            d.NC_OP_NUM_t0,
            d.AM_OP_NUM_t0,
            d.TE_OP_NUM_t0,
            d.RE_OP_NUM_t0,
            d.HE_OP_NUM_t0,
            d.PF_OP_NUM_t0,
            d.PF_OP_TOT_BAL_t0,
            d.AF_OP_NUM_t0,
            d.AM_NUM_30P_DEL_t0,
            d.TE_NUM_30P_DEL_t0,
            -- ========================================================================
            -- PMML Inputs: Direct pivots from DAS_HISTORICAL
            -- ========================================================================
            d.AM00_TRIAD_SCORE_ALIGNED_t0,
            d.AM00_CASH_AVAILABLE_t0,
            d.AM02_AMT_FCHG_PAID_LTD_t0,
            d.AM02_NUM_PAYMENTS_LTD_t0,
            d.AF_ALL_AGE_RECENT_t0,
            d.RE_ALL_AGE_OLDEST_t0,
            d.RE_ALL_AGE_RECENT_t0,
            d.MG_OP_TOT_BAL_t0,
            d.AL_NUM_INQ_6MO_t0,
            d.AM_NUM_30P_DEL_12MO_t0,
            -- ========================================================================
            -- PMML Inputs: Engineered features (pass-through from DAS_HISTORICAL)
            -- ========================================================================
            d.months_since_last_interest_charge,
            d.payment_prop_change_12m,
            d.payment_prop_change_6m,
            d.re_op_tot_utl_variance_12m,
            d.re_op_tot_utl_variance_6m,
            -- ========================================================================
            -- Fields for time-series aggregations
            -- ========================================================================
            d.RE_OP_TOT_UTL_t0,
            d.RE_OP_TOT_UTL_t1,
            d.RE_OP_TOT_UTL_t2,
            -- AL (All non-mortgage) balance fields: Calculate by subtracting MG from AM
            (
                COALESCE(SAFE_CAST(d.AM_OP_TOT_BAL_t0 AS NUMERIC), 0) - COALESCE(SAFE_CAST(d.MG_OP_TOT_BAL_t0 AS NUMERIC), 0)
            ) AS AL_OP_TOT_BAL_t0,
            (
                COALESCE(SAFE_CAST(d.AM_OP_TOT_BAL_t3 AS NUMERIC), 0) - COALESCE(SAFE_CAST(d.MG_OP_TOT_BAL_t3 AS NUMERIC), 0)
            ) AS AL_OP_TOT_BAL_t3,
            (
                COALESCE(SAFE_CAST(d.AM_OP_TOT_BAL_t6 AS NUMERIC), 0) - COALESCE(SAFE_CAST(d.MG_OP_TOT_BAL_t6 AS NUMERIC), 0)
            ) AS AL_OP_TOT_BAL_t6,
            d.AM02_AMT_PAYMENTS_LTD_t0,
            d.AM02_AMT_PAYMENTS_LTD_t3,
            d.AL_NUM_INQ_24MO_t0,
            d.AL_NUM_INQ_24MO_t1,
            d.AL_NUM_INQ_24MO_t2,
            d.AL_NUM_INQ_24MO_t3,
            d.AL_NUM_INQ_24MO_t4,
            d.AL_NUM_INQ_24MO_t5,
            d.AL_NUM_INQ_24MO_t6,
            -- ========================================================================
            -- PMML Inputs from joined tables
            -- ========================================================================
            cif.CIFP_ZIP_CODE,
            area.AREA_RANK AS area_risk_category,
            txns.total_dist_mccs,
            txns.tot_sales AS txns_tot_sales,
            txns.tot_txns AS txns_tot_txns,
            txns.merchant_risk_cat_1_trans_amt,
            txns.num_trans_risk_cat_1,
            pco.v1_mch0_salesshr_cat5_6m,
            pre_app.p6_tot_dollar_collect,
            pre_app.p6_txns_credit
        FROM
            das_historical d
            LEFT JOIN cif_snapshot cif ON SAFE_CAST(d.MAST_ACCOUNT_ID AS STRING) = SAFE_CAST(cif.MAST_ACCOUNT_ID AS STRING)
            LEFT JOIN existing_pcmc_pcma pcmc ON SAFE_CAST(d.MAST_ACCOUNT_ID AS STRING) = SAFE_CAST(pcmc.MAST_ACCOUNT_ID AS STRING)
            LEFT JOIN pre_app_score pre_app ON pcmc.wallet_id = pre_app.wallet_id
            LEFT JOIN pco_score pco ON pcmc.wallet_id = pco.wallet_id
            LEFT JOIN transaction_summary txns ON SAFE_CAST(d.MAST_ACCOUNT_ID AS STRING) = SAFE_CAST(txns.mast_account_id AS STRING)
            LEFT JOIN area_risk_lookup area ON LEFT(cif.CIFP_ZIP_CODE, 3) = area.AREA_CD
    ),
    -- ============================================================================
    -- STEP 3: Deduplicate (if multiple wallets per account, keep lowest p6_tot_sales)
    -- ============================================================================
    deduped_data AS (
        SELECT DISTINCT
            *
        EXCEPT
            (main_rank)
        FROM
            (
                SELECT DISTINCT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY MAST_ACCOUNT_ID
                        ORDER BY
                            p6_tot_sales_numeric ASC
                    ) AS main_rank
                FROM
                    base_joined_data
            )
        WHERE
            main_rank = 1
    ),
    -- ============================================================================
    -- STEP 4: Compute all PMML features
    -- ============================================================================
    transformed_ts_data AS (
        SELECT DISTINCT
            MAST_ACCOUNT_ID,
            -- ========================================================================
            -- Segmentation Variables
            -- ========================================================================
            -- TOB: Time on Books (months since account opened) - use AF_ALL_AGE_OLDEST
            CAST(
                SAFE_CAST(RE_ALL_AGE_OLDEST_t0 AS INT64) AS STRING
            ) AS TOB,
            NUM_DEROG_PUB_t0,
            CURR_PD_t0 AS CURR_PD,
            BC62_t0,
            -- ========================================================================
            -- PMML Inputs: Direct pivots from DAS_HISTORICAL
            -- ========================================================================
            AM00_TRIAD_SCORE_ALIGNED_t0,
            AM00_CASH_AVAILABLE_t0,
            AM02_AMT_FCHG_PAID_LTD_t0,
            AM02_NUM_PAYMENTS_LTD_t0,
            AF_ALL_AGE_RECENT_t0,
            RE_ALL_AGE_OLDEST_t0,
            RE_ALL_AGE_RECENT_t0,
            MG_OP_TOT_BAL_t0,
            AL_NUM_INQ_6MO_t0,
            AM_NUM_30P_DEL_12MO_t0,
            -- ========================================================================
            -- PMML Inputs: Engineered features (pass-through)
            -- ========================================================================
            months_since_last_interest_charge,
            payment_prop_change_12m,
            payment_prop_change_6m,
            re_op_tot_utl_variance_12m,
            re_op_tot_utl_variance_6m,
            -- ========================================================================
            -- PMML Inputs: 02_CRI_DATAMART Aggregations (computed inline)
            -- ========================================================================
            -- RE_OP_TOT_UTL_A3M: Average revolving utilization over 3 months
            CAST(
                (
                    COALESCE(SAFE_CAST(RE_OP_TOT_UTL_t0 AS NUMERIC), 0) + COALESCE(SAFE_CAST(RE_OP_TOT_UTL_t1 AS NUMERIC), 0) + COALESCE(SAFE_CAST(RE_OP_TOT_UTL_t2 AS NUMERIC), 0)
                ) / 3 AS STRING
            ) AS RE_OP_TOT_UTL_A3M,
            -- AL_OP_TOT_BAL_Qtr: All open total balance quarterly change (t0 - t3)
            CAST(
                (
                    SAFE_CAST(AL_OP_TOT_BAL_t0 AS NUMERIC) - SAFE_CAST(AL_OP_TOT_BAL_t3 AS NUMERIC)
                ) AS STRING
            ) AS AL_OP_TOT_BAL_Qtr,
            -- AM02_AMT_PAYMENTS_LTD_SL3M: Payments LTD slope over 3 months
            CAST(
                (
                    SAFE_CAST(AM02_AMT_PAYMENTS_LTD_t0 AS NUMERIC) - SAFE_CAST(AM02_AMT_PAYMENTS_LTD_t3 AS NUMERIC)
                ) / 3 AS STRING
            ) AS AM02_AMT_PAYMENTS_LTD_SL3M,
            -- AL_NUM_INQ_24MO_A6M: All inquiries 24mo average over 6 months
            CAST(
                (
                    COALESCE(SAFE_CAST(AL_NUM_INQ_24MO_t0 AS NUMERIC), 0) + COALESCE(SAFE_CAST(AL_NUM_INQ_24MO_t1 AS NUMERIC), 0) + COALESCE(SAFE_CAST(AL_NUM_INQ_24MO_t2 AS NUMERIC), 0) + COALESCE(SAFE_CAST(AL_NUM_INQ_24MO_t3 AS NUMERIC), 0) + COALESCE(SAFE_CAST(AL_NUM_INQ_24MO_t4 AS NUMERIC), 0) + COALESCE(SAFE_CAST(AL_NUM_INQ_24MO_t5 AS NUMERIC), 0)
                ) / 6 AS STRING
            ) AS AL_NUM_INQ_24MO_A6M,
            -- AL_NUM_INQ_24MO_2Qtr: All inquiries 24mo 6-month change (t0 - t6)
            CAST(
                (
                    SAFE_CAST(AL_NUM_INQ_24MO_t0 AS NUMERIC) - SAFE_CAST(AL_NUM_INQ_24MO_t6 AS NUMERIC)
                ) AS STRING
            ) AS AL_NUM_INQ_24MO_2Qtr,
            -- AL_OP_TOT_BAL_2Qtr: All open total balance 6-month change (t0 - t6)
            CAST(
                (
                    SAFE_CAST(AL_OP_TOT_BAL_t0 AS NUMERIC) - SAFE_CAST(AL_OP_TOT_BAL_t6 AS NUMERIC)
                ) AS STRING
            ) AS AL_OP_TOT_BAL_2Qtr,
            -- ========================================================================
            -- PMML Inputs: From joined tables
            -- ========================================================================
            -- area_risk_category: Area-level risk ranking
            CAST(area_risk_category AS STRING) AS area_risk_category,
            -- product_hierarchy_rank: Product hierarchy based on credit product mix
            CAST(
                CASE
                    WHEN COALESCE(SAFE_CAST(MG_OP_NUM_t0 AS NUMERIC), 0) > 0 THEN 1 -- Mortgage
                    WHEN (
                        COALESCE(SAFE_CAST(NC_OP_NUM_t0 AS NUMERIC), 0) > 0
                        AND COALESCE(SAFE_CAST(NC_OP_NUM_t0 AS NUMERIC), 0) = (
                            COALESCE(SAFE_CAST(AM_OP_NUM_t0 AS NUMERIC), 0) + COALESCE(SAFE_CAST(TE_OP_NUM_t0 AS NUMERIC), 0)
                        )
                    )
                    OR (
                        COALESCE(SAFE_CAST(RE_OP_NUM_t0 AS NUMERIC), 0) > 0
                        AND COALESCE(SAFE_CAST(RE_OP_NUM_t0 AS NUMERIC), 0) = (
                            COALESCE(SAFE_CAST(AM_OP_NUM_t0 AS NUMERIC), 0) + COALESCE(SAFE_CAST(TE_OP_NUM_t0 AS NUMERIC), 0)
                        )
                    ) THEN 2 -- No credit or revolving only
                    WHEN COALESCE(SAFE_CAST(HE_OP_NUM_t0 AS NUMERIC), 0) > 0 THEN 3 -- HELOC
                    WHEN COALESCE(SAFE_CAST(PF_OP_NUM_t0 AS NUMERIC), 0) > 0
                    AND COALESCE(SAFE_CAST(PF_OP_TOT_BAL_t0 AS NUMERIC), 0) < 1000 THEN 4 -- Small personal loan
                    WHEN COALESCE(SAFE_CAST(AF_OP_NUM_t0 AS NUMERIC), 0) > 0 THEN 5 -- Auto finance
                    WHEN COALESCE(SAFE_CAST(PF_OP_NUM_t0 AS NUMERIC), 0) > 0
                    AND COALESCE(SAFE_CAST(PF_OP_TOT_BAL_t0 AS NUMERIC), 0) >= 1000 THEN 6 -- Large personal loan
                    ELSE NULL
                END AS STRING
            ) AS product_hierarchy_rank,
            -- prop_clean_live: Proportion of accounts without 30+ day delinquency
            CAST(
                CASE
                    WHEN (
                        COALESCE(SAFE_CAST(AM_OP_NUM_t0 AS NUMERIC), 0) + COALESCE(SAFE_CAST(TE_OP_NUM_t0 AS NUMERIC), 0)
                    ) > 0 THEN LEAST(
                        1.0,
                        GREATEST(
                            0.0,
                            1.0 - (
                                (
                                    COALESCE(SAFE_CAST(AM_NUM_30P_DEL_t0 AS NUMERIC), 0) + COALESCE(SAFE_CAST(TE_NUM_30P_DEL_t0 AS NUMERIC), 0)
                                ) * 1.0 / (
                                    COALESCE(SAFE_CAST(AM_OP_NUM_t0 AS NUMERIC), 0) + COALESCE(SAFE_CAST(TE_OP_NUM_t0 AS NUMERIC), 0)
                                )
                            )
                        )
                    )
                    ELSE NULL
                END AS STRING
            ) AS prop_clean_live,
            -- prop_txns_risk_cat_1: Proportion of transactions in risk category 1 (luxury)
            CAST(
                SAFE_DIVIDE(
                    SAFE_CAST(num_trans_risk_cat_1 AS NUMERIC),
                    SAFE_CAST(txns_tot_txns AS NUMERIC)
                ) AS STRING
            ) AS prop_txns_risk_cat_1,
            -- prop_sales_risk_cat_1: Proportion of sales in risk category 1 (luxury)
            CAST(
                SAFE_DIVIDE(
                    SAFE_CAST(merchant_risk_cat_1_trans_amt AS NUMERIC),
                    SAFE_CAST(txns_tot_sales AS NUMERIC)
                ) AS STRING
            ) AS prop_sales_risk_cat_1,
            -- total_dist_mccs: Total distinct merchant category codes
            CAST(total_dist_mccs AS STRING) AS total_dist_mccs,
            -- v1_mch0_salesshr_cat5_6m: MCC category 5 sales share (groceries)
            CAST(v1_mch0_salesshr_cat5_6m AS STRING) AS v1_mch0_salesshr_cat5_6m,
            -- p6_tot_dollar_collect: Total dollar amount collected (6 months)
            CAST(p6_tot_dollar_collect AS STRING) AS p6_tot_dollar_collect,
            -- p6_txns_credit: Total credit transactions (6 months)
            CAST(p6_txns_credit AS STRING) AS p6_txns_credit
        FROM
            deduped_data
    ),
    -- ============================================================================
    -- STEP 5: Transform wide format (35 columns) to narrow format (REPEATED RECORD)
    -- ============================================================================
    narrow_features AS (
        SELECT DISTINCT
            -- Create array of feature name-value pairs
            ARRAY [
                -- Segmentation variables
                STRUCT('MAST_ACCOUNT_ID' AS FEATURE_NAME, MAST_ACCOUNT_ID AS FEATURE_VALUE),
                STRUCT('TOB' AS FEATURE_NAME, TOB AS FEATURE_VALUE),
                STRUCT('NUM_DEROG_PUB_t0' AS FEATURE_NAME, NUM_DEROG_PUB_t0 AS FEATURE_VALUE),
                STRUCT('CURR_PD' AS FEATURE_NAME, CURR_PD AS FEATURE_VALUE),
                STRUCT('BC62_t0' AS FEATURE_NAME, BC62_t0 AS FEATURE_VALUE),
                -- Direct pivots
                STRUCT('AM00_TRIAD_SCORE_ALIGNED_t0' AS FEATURE_NAME, AM00_TRIAD_SCORE_ALIGNED_t0 AS FEATURE_VALUE),
                STRUCT('AM00_CASH_AVAILABLE_t0' AS FEATURE_NAME, AM00_CASH_AVAILABLE_t0 AS FEATURE_VALUE),
                STRUCT('AM02_AMT_FCHG_PAID_LTD_t0' AS FEATURE_NAME, AM02_AMT_FCHG_PAID_LTD_t0 AS FEATURE_VALUE),
                STRUCT('AM02_NUM_PAYMENTS_LTD_t0' AS FEATURE_NAME, AM02_NUM_PAYMENTS_LTD_t0 AS FEATURE_VALUE),
                STRUCT('AF_ALL_AGE_RECENT_t0' AS FEATURE_NAME, AF_ALL_AGE_RECENT_t0 AS FEATURE_VALUE),
                STRUCT('RE_ALL_AGE_OLDEST_t0' AS FEATURE_NAME, RE_ALL_AGE_OLDEST_t0 AS FEATURE_VALUE),
                STRUCT('RE_ALL_AGE_RECENT_t0' AS FEATURE_NAME, RE_ALL_AGE_RECENT_t0 AS FEATURE_VALUE),
                STRUCT('MG_OP_TOT_BAL_t0' AS FEATURE_NAME, MG_OP_TOT_BAL_t0 AS FEATURE_VALUE),
                STRUCT('AL_NUM_INQ_6MO_t0' AS FEATURE_NAME, AL_NUM_INQ_6MO_t0 AS FEATURE_VALUE),
                STRUCT('AM_NUM_30P_DEL_12MO_t0' AS FEATURE_NAME, AM_NUM_30P_DEL_12MO_t0 AS FEATURE_VALUE),
                -- Engineered features
                STRUCT('months_since_last_interest_charge' AS FEATURE_NAME, months_since_last_interest_charge AS FEATURE_VALUE),
                STRUCT('payment_prop_change_12m' AS FEATURE_NAME, payment_prop_change_12m AS FEATURE_VALUE),
                STRUCT('payment_prop_change_6m' AS FEATURE_NAME, payment_prop_change_6m AS FEATURE_VALUE),
                STRUCT('re_op_tot_utl_variance_12m' AS FEATURE_NAME, re_op_tot_utl_variance_12m AS FEATURE_VALUE),
                STRUCT('re_op_tot_utl_variance_6m' AS FEATURE_NAME, re_op_tot_utl_variance_6m AS FEATURE_VALUE),
                -- Aggregations
                STRUCT('RE_OP_TOT_UTL_A3M' AS FEATURE_NAME, RE_OP_TOT_UTL_A3M AS FEATURE_VALUE),
                STRUCT('AL_OP_TOT_BAL_Qtr' AS FEATURE_NAME, AL_OP_TOT_BAL_Qtr AS FEATURE_VALUE),
                STRUCT('AM02_AMT_PAYMENTS_LTD_SL3M' AS FEATURE_NAME, AM02_AMT_PAYMENTS_LTD_SL3M AS FEATURE_VALUE),
                STRUCT('AL_NUM_INQ_24MO_A6M' AS FEATURE_NAME, AL_NUM_INQ_24MO_A6M AS FEATURE_VALUE),
                STRUCT('AL_NUM_INQ_24MO_2Qtr' AS FEATURE_NAME, AL_NUM_INQ_24MO_2Qtr AS FEATURE_VALUE),
                STRUCT('AL_OP_TOT_BAL_2Qtr' AS FEATURE_NAME, AL_OP_TOT_BAL_2Qtr AS FEATURE_VALUE),
                -- Transaction/demographic (placeholder - update with actual joins)
                STRUCT('area_risk_category' AS FEATURE_NAME, area_risk_category AS FEATURE_VALUE),
                STRUCT('product_hierarchy_rank' AS FEATURE_NAME, product_hierarchy_rank AS FEATURE_VALUE),
                STRUCT('prop_clean_live' AS FEATURE_NAME, prop_clean_live AS FEATURE_VALUE),
                STRUCT('prop_txns_risk_cat_1' AS FEATURE_NAME, prop_txns_risk_cat_1 AS FEATURE_VALUE),
                STRUCT('prop_sales_risk_cat_1' AS FEATURE_NAME, prop_sales_risk_cat_1 AS FEATURE_VALUE),
                STRUCT('total_dist_mccs' AS FEATURE_NAME, total_dist_mccs AS FEATURE_VALUE),
                STRUCT('v1_mch0_salesshr_cat5_6m' AS FEATURE_NAME, v1_mch0_salesshr_cat5_6m AS FEATURE_VALUE),
                STRUCT('p6_tot_dollar_collect' AS FEATURE_NAME, p6_tot_dollar_collect AS FEATURE_VALUE),
                STRUCT('p6_txns_credit' AS FEATURE_NAME, p6_txns_credit AS FEATURE_VALUE)
            ] AS FEATURES,
            CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
            '{dag_id}' AS JOB_ID
        FROM
            transformed_ts_data
    ) -- ============================================================================
    -- STEP 6: Insert into target table
    -- ============================================================================
SELECT
    *
FROM
    narrow_features;