-- ============================================================================
-- TRS T0090: Insert Unnested Output Table (Wide Format)
-- ============================================================================
INSERT INTO
    `pcb-{env}-landing.domain_scoring.OUTPUT_TRS_T0090_UNNESTED`
    (
    MAST_ACCOUNT_ID,
    TOB,
    OPEN_TOB,
    N_TOT_TRANS,
    TRNS_PER_MTH,
    TOT_SPEND,
    SPEND_PER_TRNS,
    TOT_GROCERY_SPEND,
    TOT_GROCERY_TRANS,
    GROCERY_TRANS_PER_MONTH,
    GROCERY_SPEND_PER_MONTH,
    N_CASH_ADVANCES,
    TOT_CASH_ADVANCES,
    CASH_PCT_OF_SPEND,
    CASH_PCT_OF_TRNS,
    TOT_NON_LCL_SPEND,
    N_NON_LCL_TRANS,
    AVG_NON_LCL_SPEND_PER_TRANS,
    AVG_NON_LCL_SPEND_PER_MONTH,
    TOT_LCL_SPEND,
    N_LCL_TRANS,
    AVG_LCL_SPEND_PER_TRANS,
    AVG_LCL_SPEND_PER_MONTH,
    LCL_PCT_OF_SPEND,
    NON_LCL_PCT_OF_SPEND,
    LCL_PCT_OF_TRANS,
    NON_LCL_PCT_OF_TRANS,
    GROCERY_PCT_OF_SPEND,
    GROCERY_PCT_OF_TRNS,
    TOT_FFOOD_SPEND,
    TOT_GAS_SPEND,
    TOT_ELCTRNCS_SPEND,
    TOT_TELCOSALE_SPEND,
    TOT_FAMCLOTHES_SPEND,
    TOT_CONV_SPEND,
    N_HARDWARE_TRNS,
    N_GAS_TRNS,
    N_FFOOD_TRNS,
    N_RESTO_TRNS,
    N_PHARM_TRNS,
    N_LICQ_TRNS,
    N_PARKING_TRNS,
    N_ELECTRNCS_TRNS,
    N_VARIETY_TRNS,
    N_FAMCLOTHES_TRNS,
    N_CONV_TRNS,
    N_AIRLINE_TRNS,
    N_BOOK_TRNS,
    N_CLOTHES_TRNS,
    N_COSMETIC_TRNS,
    N_DISCOUNT_TRNS,
    N_FURNITURE_TRNS,
    N_GAMES_TRNS,
    N_GOV_TRNS,
    N_HOTEL_TRNS,
    N_INSURANCE_TRNS,
    N_ITSERVICE_TRNS,
    N_MISC_TRNS,
    N_RENO_TRNS,
    N_SALON_TRNS,
    N_SPORTG_TRNS,
    N_FRTWCLOTHES_TRNS,
    N_TAXI_TRNS,
    PCT_FFOOD_SPEND,
    PCT_GAS_TRNS,
    PCT_GAS_SPEND,
    PCT_RESTO_TRNS,
    PCT_PHARM_TRNS,
    PCT_LICQ_TRNS,
    PCT_ELCTRNCS_TRNS,
    PCT_ELCTRNCS_SPEND,
    PCT_TELCOSALE_SPEND,
    PCT_FAMCLOTHES_SPEND,
    PCT_CONV_SPEND,
    FFOOD_IND,
    GAS_IND,
    RESTO_IND,
    HARDWARE_IND,
    VARIETY_IND,
    FAMCLOTHES_IND,
    CONV_IND,
    AIRLINE_IND,
    BOOK_IND,
    CLOTHES_IND,
    COSMETIC_IND,
    DISCOUNT_IND,
    FURNITURE_IND,
    GAMES_IND,
    GOV_IND,
    HOTEL_IND,
    INSURANCE_IND,
    ITSERVICE_IND,
    MISC_IND,
    RENO_IND,
    SALON_IND,
    SPORTG_IND,
    FRTWCLOTHES_IND,
    TAXI_IND,
    TRANS_CNT_3MTH,
    TRANS_CNT_6MTH,
    PYMT_RATIO,
    UTIL,
    CIFP_TEST_DIGIT_TYPE5,
    CIFP_ACQUISITION_CODE,
    CIFP_ACQUISITION_CODE_SIGN,
    MORTGAGE_IND,
    MORTGAGE_IND_SIGN,
    APP_BCN,
    APP_BCN_SIGN,
    TTLPOINTS,
    TTLPOINTS_SIGN,
    PRIZM,
    IAL_INQS_PST12_MNTH,
    TNC_TRD_CURR_PST_DUE,
    TNC_UTIL_PERCENTAGE_OPEN_TRD,
    SCORECARD,
    RAW_RESULT,
    SCORECARD_POINTS,
    SCORE_RUN_COUNTER,
    REC_LOAD_TIMESTAMP,
    JOB_ID
)
-- ==========================================================================
-- Single source: OUTPUT_TRS_T0090_LATEST (all input + output fields in OUTPUTS).
-- Pivot OUTPUT_NAME/OUTPUT_VALUE into wide columns (same approach as all_score_trs_t0090_extract).
-- No dependency on INPUT_TRS_T0090_FEATURES / INPUT_TRS_T0090_FEATURES_LATEST.
-- ==========================================================================
WITH
    output_with_row_id AS (
        SELECT
            ROW_NUMBER() OVER () AS source_row_id,
            r.EXECUTION_ID,
            r.REC_LOAD_TIMESTAMP,
            r.JOB_ID,
            r.OUTPUTS
        FROM
            `pcb-{env}-curated.domain_scoring.OUTPUT_TRS_T0090_LATEST` AS r
    ),
    trs_pivoted AS (
        SELECT
            ANY_VALUE(r.REC_LOAD_TIMESTAMP) AS REC_LOAD_TIMESTAMP,
            ANY_VALUE(r.JOB_ID) AS JOB_ID,
            -- All fields from OUTPUTS (input features + model outputs)
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'MAST_ACCOUNT_ID' THEN o.OUTPUT_VALUE
                END
            ) AS MAST_ACCOUNT_ID,
            -- Transaction Summary Features
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'TOB' THEN o.OUTPUT_VALUE
                END
            ) AS TOB,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'OPEN_TOB' THEN o.OUTPUT_VALUE
                END
            ) AS OPEN_TOB,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_TOT_TRANS' THEN o.OUTPUT_VALUE
                END
            ) AS N_TOT_TRANS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'TRNS_PER_MTH' THEN o.OUTPUT_VALUE
                END
            ) AS TRNS_PER_MTH,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'TOT_SPEND' THEN o.OUTPUT_VALUE
                END
            ) AS TOT_SPEND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'SPEND_PER_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS SPEND_PER_TRNS,
            -- Grocery Features
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'TOT_GROCERY_SPEND' THEN o.OUTPUT_VALUE
                END
            ) AS TOT_GROCERY_SPEND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'TOT_GROCERY_TRANS' THEN o.OUTPUT_VALUE
                END
            ) AS TOT_GROCERY_TRANS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'GROCERY_TRANS_PER_MONTH' THEN o.OUTPUT_VALUE
                END
            ) AS GROCERY_TRANS_PER_MONTH,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'GROCERY_SPEND_PER_MONTH' THEN o.OUTPUT_VALUE
                END
            ) AS GROCERY_SPEND_PER_MONTH,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'GROCERY_PCT_OF_SPEND' THEN o.OUTPUT_VALUE
                END
            ) AS GROCERY_PCT_OF_SPEND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'GROCERY_PCT_OF_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS GROCERY_PCT_OF_TRNS,
            -- Cash Advance Features
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_CASH_ADVANCES' THEN o.OUTPUT_VALUE
                END
            ) AS N_CASH_ADVANCES,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'TOT_CASH_ADVANCES' THEN o.OUTPUT_VALUE
                END
            ) AS TOT_CASH_ADVANCES,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'CASH_PCT_OF_SPEND' THEN o.OUTPUT_VALUE
                END
            ) AS CASH_PCT_OF_SPEND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'CASH_PCT_OF_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS CASH_PCT_OF_TRNS,
            -- Local vs Non-Local Features
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'TOT_NON_LCL_SPEND' THEN o.OUTPUT_VALUE
                END
            ) AS TOT_NON_LCL_SPEND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_NON_LCL_TRANS' THEN o.OUTPUT_VALUE
                END
            ) AS N_NON_LCL_TRANS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'AVG_NON_LCL_SPEND_PER_TRANS' THEN o.OUTPUT_VALUE
                END
            ) AS AVG_NON_LCL_SPEND_PER_TRANS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'AVG_NON_LCL_SPEND_PER_MONTH' THEN o.OUTPUT_VALUE
                END
            ) AS AVG_NON_LCL_SPEND_PER_MONTH,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'TOT_LCL_SPEND' THEN o.OUTPUT_VALUE
                END
            ) AS TOT_LCL_SPEND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_LCL_TRANS' THEN o.OUTPUT_VALUE
                END
            ) AS N_LCL_TRANS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'AVG_LCL_SPEND_PER_TRANS' THEN o.OUTPUT_VALUE
                END
            ) AS AVG_LCL_SPEND_PER_TRANS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'AVG_LCL_SPEND_PER_MONTH' THEN o.OUTPUT_VALUE
                END
            ) AS AVG_LCL_SPEND_PER_MONTH,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'LCL_PCT_OF_SPEND' THEN o.OUTPUT_VALUE
                END
            ) AS LCL_PCT_OF_SPEND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'NON_LCL_PCT_OF_SPEND' THEN o.OUTPUT_VALUE
                END
            ) AS NON_LCL_PCT_OF_SPEND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'LCL_PCT_OF_TRANS' THEN o.OUTPUT_VALUE
                END
            ) AS LCL_PCT_OF_TRANS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'NON_LCL_PCT_OF_TRANS' THEN o.OUTPUT_VALUE
                END
            ) AS NON_LCL_PCT_OF_TRANS,
            -- Merchant Category Spend Totals
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'TOT_FFOOD_SPEND' THEN o.OUTPUT_VALUE
                END
            ) AS TOT_FFOOD_SPEND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'TOT_GAS_SPEND' THEN o.OUTPUT_VALUE
                END
            ) AS TOT_GAS_SPEND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'TOT_ELCTRNCS_SPEND' THEN o.OUTPUT_VALUE
                END
            ) AS TOT_ELCTRNCS_SPEND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'TOT_TELCOSALE_SPEND' THEN o.OUTPUT_VALUE
                END
            ) AS TOT_TELCOSALE_SPEND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'TOT_FAMCLOTHES_SPEND' THEN o.OUTPUT_VALUE
                END
            ) AS TOT_FAMCLOTHES_SPEND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'TOT_CONV_SPEND' THEN o.OUTPUT_VALUE
                END
            ) AS TOT_CONV_SPEND,
            -- Merchant Category Transaction Counts
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_HARDWARE_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_HARDWARE_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_GAS_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_GAS_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_FFOOD_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_FFOOD_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_RESTO_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_RESTO_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_PHARM_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_PHARM_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_LICQ_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_LICQ_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_PARKING_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_PARKING_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_ELECTRNCS_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_ELECTRNCS_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_VARIETY_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_VARIETY_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_FAMCLOTHES_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_FAMCLOTHES_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_CONV_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_CONV_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_AIRLINE_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_AIRLINE_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_BOOK_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_BOOK_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_CLOTHES_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_CLOTHES_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_COSMETIC_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_COSMETIC_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_DISCOUNT_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_DISCOUNT_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_FURNITURE_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_FURNITURE_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_GAMES_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_GAMES_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_GOV_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_GOV_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_HOTEL_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_HOTEL_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_INSURANCE_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_INSURANCE_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_ITSERVICE_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_ITSERVICE_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_MISC_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_MISC_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_RENO_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_RENO_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_SALON_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_SALON_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_SPORTG_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_SPORTG_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_FRTWCLOTHES_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_FRTWCLOTHES_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'N_TAXI_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS N_TAXI_TRNS,
            -- Merchant Category Percentages
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'PCT_FFOOD_SPEND' THEN o.OUTPUT_VALUE
                END
            ) AS PCT_FFOOD_SPEND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'PCT_GAS_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS PCT_GAS_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'PCT_GAS_SPEND' THEN o.OUTPUT_VALUE
                END
            ) AS PCT_GAS_SPEND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'PCT_RESTO_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS PCT_RESTO_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'PCT_PHARM_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS PCT_PHARM_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'PCT_LICQ_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS PCT_LICQ_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'PCT_ELCTRNCS_TRNS' THEN o.OUTPUT_VALUE
                END
            ) AS PCT_ELCTRNCS_TRNS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'PCT_ELCTRNCS_SPEND' THEN o.OUTPUT_VALUE
                END
            ) AS PCT_ELCTRNCS_SPEND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'PCT_TELCOSALE_SPEND' THEN o.OUTPUT_VALUE
                END
            ) AS PCT_TELCOSALE_SPEND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'PCT_FAMCLOTHES_SPEND' THEN o.OUTPUT_VALUE
                END
            ) AS PCT_FAMCLOTHES_SPEND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'PCT_CONV_SPEND' THEN o.OUTPUT_VALUE
                END
            ) AS PCT_CONV_SPEND,
            -- Merchant Category Indicators
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'FFOOD_IND' THEN o.OUTPUT_VALUE
                END
            ) AS FFOOD_IND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'GAS_IND' THEN o.OUTPUT_VALUE
                END
            ) AS GAS_IND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'RESTO_IND' THEN o.OUTPUT_VALUE
                END
            ) AS RESTO_IND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'HARDWARE_IND' THEN o.OUTPUT_VALUE
                END
            ) AS HARDWARE_IND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'VARIETY_IND' THEN o.OUTPUT_VALUE
                END
            ) AS VARIETY_IND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'FAMCLOTHES_IND' THEN o.OUTPUT_VALUE
                END
            ) AS FAMCLOTHES_IND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'CONV_IND' THEN o.OUTPUT_VALUE
                END
            ) AS CONV_IND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'AIRLINE_IND' THEN o.OUTPUT_VALUE
                END
            ) AS AIRLINE_IND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'BOOK_IND' THEN o.OUTPUT_VALUE
                END
            ) AS BOOK_IND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'CLOTHES_IND' THEN o.OUTPUT_VALUE
                END
            ) AS CLOTHES_IND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'COSMETIC_IND' THEN o.OUTPUT_VALUE
                END
            ) AS COSMETIC_IND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'DISCOUNT_IND' THEN o.OUTPUT_VALUE
                END
            ) AS DISCOUNT_IND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'FURNITURE_IND' THEN o.OUTPUT_VALUE
                END
            ) AS FURNITURE_IND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'GAMES_IND' THEN o.OUTPUT_VALUE
                END
            ) AS GAMES_IND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'GOV_IND' THEN o.OUTPUT_VALUE
                END
            ) AS GOV_IND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'HOTEL_IND' THEN o.OUTPUT_VALUE
                END
            ) AS HOTEL_IND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'INSURANCE_IND' THEN o.OUTPUT_VALUE
                END
            ) AS INSURANCE_IND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'ITSERVICE_IND' THEN o.OUTPUT_VALUE
                END
            ) AS ITSERVICE_IND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'MISC_IND' THEN o.OUTPUT_VALUE
                END
            ) AS MISC_IND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'RENO_IND' THEN o.OUTPUT_VALUE
                END
            ) AS RENO_IND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'SALON_IND' THEN o.OUTPUT_VALUE
                END
            ) AS SALON_IND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'SPORTG_IND' THEN o.OUTPUT_VALUE
                END
            ) AS SPORTG_IND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'FRTWCLOTHES_IND' THEN o.OUTPUT_VALUE
                END
            ) AS FRTWCLOTHES_IND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'TAXI_IND' THEN o.OUTPUT_VALUE
                END
            ) AS TAXI_IND,
            -- Time Period Features
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'TRANS_CNT_3MTH' THEN o.OUTPUT_VALUE
                END
            ) AS TRANS_CNT_3MTH,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'TRANS_CNT_6MTH' THEN o.OUTPUT_VALUE
                END
            ) AS TRANS_CNT_6MTH,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'PYMT_RATIO' THEN o.OUTPUT_VALUE
                END
            ) AS PYMT_RATIO,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'UTIL' THEN o.OUTPUT_VALUE
                END
            ) AS UTIL,
            -- Customer Profile Features
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'CIFP_TEST_DIGIT_TYPE5' THEN o.OUTPUT_VALUE
                END
            ) AS CIFP_TEST_DIGIT_TYPE5,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'CIFP_ACQUISITION_CODE' THEN o.OUTPUT_VALUE
                END
            ) AS CIFP_ACQUISITION_CODE,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'CIFP_ACQUISITION_CODE_SIGN' THEN o.OUTPUT_VALUE
                END
            ) AS CIFP_ACQUISITION_CODE_SIGN,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'MORTGAGE_IND' THEN o.OUTPUT_VALUE
                END
            ) AS MORTGAGE_IND,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'MORTGAGE_IND_SIGN' THEN o.OUTPUT_VALUE
                END
            ) AS MORTGAGE_IND_SIGN,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'APP_BCN' THEN o.OUTPUT_VALUE
                END
            ) AS APP_BCN,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'APP_BCN_SIGN' THEN o.OUTPUT_VALUE
                END
            ) AS APP_BCN_SIGN,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'TTLPOINTS' THEN o.OUTPUT_VALUE
                END
            ) AS TTLPOINTS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'TTLPOINTS_SIGN' THEN o.OUTPUT_VALUE
                END
            ) AS TTLPOINTS_SIGN,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'PRIZM' THEN o.OUTPUT_VALUE
                END
            ) AS PRIZM,
            -- Credit Bureau Features
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'IAL_INQS_PST12_MNTH' THEN o.OUTPUT_VALUE
                END
            ) AS IAL_INQS_PST12_MNTH,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'TNC_TRD_CURR_PST_DUE' THEN o.OUTPUT_VALUE
                END
            ) AS TNC_TRD_CURR_PST_DUE,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'TNC_UTIL_PERCENTAGE_OPEN_TRD' THEN o.OUTPUT_VALUE
                END
            ) AS TNC_UTIL_PERCENTAGE_OPEN_TRD,
            -- Scorecard Segment
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'SCORECARD' THEN o.OUTPUT_VALUE
                END
            ) AS SCORECARD,
            -- PMML model outputs (also in OUTPUTS)
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'RAWRESULT' THEN o.OUTPUT_VALUE
                END
            ) AS RAW_RESULT,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'SCORECARD_POINTS' THEN o.OUTPUT_VALUE
                END
            ) AS SCORECARD_POINTS,
            MAX(
                CASE
                    WHEN UPPER(o.OUTPUT_NAME) = 'SCORE_RUN_COUNTER' THEN o.OUTPUT_VALUE
                END
            ) AS SCORE_RUN_COUNTER
        FROM
            output_with_row_id AS r
            CROSS JOIN UNNEST(r.OUTPUTS) AS o
        GROUP BY
            r.source_row_id
    )
SELECT DISTINCT
    p.MAST_ACCOUNT_ID,
    p.TOB,
    p.OPEN_TOB,
    p.N_TOT_TRANS,
    p.TRNS_PER_MTH,
    p.TOT_SPEND,
    p.SPEND_PER_TRNS,
    p.TOT_GROCERY_SPEND,
    p.TOT_GROCERY_TRANS,
    p.GROCERY_TRANS_PER_MONTH,
    p.GROCERY_SPEND_PER_MONTH,
    p.N_CASH_ADVANCES,
    p.TOT_CASH_ADVANCES,
    p.CASH_PCT_OF_SPEND,
    p.CASH_PCT_OF_TRNS,
    p.TOT_NON_LCL_SPEND,
    p.N_NON_LCL_TRANS,
    p.AVG_NON_LCL_SPEND_PER_TRANS,
    p.AVG_NON_LCL_SPEND_PER_MONTH,
    p.TOT_LCL_SPEND,
    p.N_LCL_TRANS,
    p.AVG_LCL_SPEND_PER_TRANS,
    p.AVG_LCL_SPEND_PER_MONTH,
    p.LCL_PCT_OF_SPEND,
    p.NON_LCL_PCT_OF_SPEND,
    p.LCL_PCT_OF_TRANS,
    p.NON_LCL_PCT_OF_TRANS,
    p.GROCERY_PCT_OF_SPEND,
    p.GROCERY_PCT_OF_TRNS,
    p.TOT_FFOOD_SPEND,
    p.TOT_GAS_SPEND,
    p.TOT_ELCTRNCS_SPEND,
    p.TOT_TELCOSALE_SPEND,
    p.TOT_FAMCLOTHES_SPEND,
    p.TOT_CONV_SPEND,
    p.N_HARDWARE_TRNS,
    p.N_GAS_TRNS,
    p.N_FFOOD_TRNS,
    p.N_RESTO_TRNS,
    p.N_PHARM_TRNS,
    p.N_LICQ_TRNS,
    p.N_PARKING_TRNS,
    p.N_ELECTRNCS_TRNS,
    p.N_VARIETY_TRNS,
    p.N_FAMCLOTHES_TRNS,
    p.N_CONV_TRNS,
    p.N_AIRLINE_TRNS,
    p.N_BOOK_TRNS,
    p.N_CLOTHES_TRNS,
    p.N_COSMETIC_TRNS,
    p.N_DISCOUNT_TRNS,
    p.N_FURNITURE_TRNS,
    p.N_GAMES_TRNS,
    p.N_GOV_TRNS,
    p.N_HOTEL_TRNS,
    p.N_INSURANCE_TRNS,
    p.N_ITSERVICE_TRNS,
    p.N_MISC_TRNS,
    p.N_RENO_TRNS,
    p.N_SALON_TRNS,
    p.N_SPORTG_TRNS,
    p.N_FRTWCLOTHES_TRNS,
    p.N_TAXI_TRNS,
    p.PCT_FFOOD_SPEND,
    p.PCT_GAS_TRNS,
    p.PCT_GAS_SPEND,
    p.PCT_RESTO_TRNS,
    p.PCT_PHARM_TRNS,
    p.PCT_LICQ_TRNS,
    p.PCT_ELCTRNCS_TRNS,
    p.PCT_ELCTRNCS_SPEND,
    p.PCT_TELCOSALE_SPEND,
    p.PCT_FAMCLOTHES_SPEND,
    p.PCT_CONV_SPEND,
    p.FFOOD_IND,
    p.GAS_IND,
    p.RESTO_IND,
    p.HARDWARE_IND,
    p.VARIETY_IND,
    p.FAMCLOTHES_IND,
    p.CONV_IND,
    p.AIRLINE_IND,
    p.BOOK_IND,
    p.CLOTHES_IND,
    p.COSMETIC_IND,
    p.DISCOUNT_IND,
    p.FURNITURE_IND,
    p.GAMES_IND,
    p.GOV_IND,
    p.HOTEL_IND,
    p.INSURANCE_IND,
    p.ITSERVICE_IND,
    p.MISC_IND,
    p.RENO_IND,
    p.SALON_IND,
    p.SPORTG_IND,
    p.FRTWCLOTHES_IND,
    p.TAXI_IND,
    p.TRANS_CNT_3MTH,
    p.TRANS_CNT_6MTH,
    p.PYMT_RATIO,
    p.UTIL,
    p.CIFP_TEST_DIGIT_TYPE5,
    p.CIFP_ACQUISITION_CODE,
    p.CIFP_ACQUISITION_CODE_SIGN,
    p.MORTGAGE_IND,
    p.MORTGAGE_IND_SIGN,
    p.APP_BCN,
    p.APP_BCN_SIGN,
    p.TTLPOINTS,
    p.TTLPOINTS_SIGN,
    p.PRIZM,
    p.IAL_INQS_PST12_MNTH,
    p.TNC_TRD_CURR_PST_DUE,
    p.TNC_UTIL_PERCENTAGE_OPEN_TRD,
    p.SCORECARD,
    p.RAW_RESULT,
    p.SCORECARD_POINTS,
    p.SCORE_RUN_COUNTER,
    -- Audit fields
    CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
    '{dag_id}' AS JOB_ID
FROM
    trs_pivoted p;