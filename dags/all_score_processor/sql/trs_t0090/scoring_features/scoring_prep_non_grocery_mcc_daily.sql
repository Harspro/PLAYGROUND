INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_NON_GROCERY_MCC_DAILY`
(
  MAST_ACCOUNT_ID,
  N_TOT_TRANS,
  TOT_SPEND,
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
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
WITH
  base_data AS (
    SELECT DISTINCT
      CAST(MAST_ACCOUNT_ID AS NUMERIC) AS MAST_ACCOUNT_ID,
      MERCH2,
      CAST(TRANS_AMT AS NUMERIC) AS TRANS_AMT,
      CAST(MCC AS NUMERIC) AS MCC
    FROM
      `pcb-{env}-curated.domain_scoring.SCORING_PREP_CUST_TRANS_FULL_DAILY_LATEST`
  ),
  enriched_data_1 AS (
    SELECT DISTINCT
      MAST_ACCOUNT_ID,
      COUNT(MERCH2) AS N_TOT_TRANS,
      SUM(TRANS_AMT) AS TOT_SPEND,
      SUM(
        CASE
          WHEN MCC = 5542 THEN TRANS_AMT
          ELSE 0
          END) AS TOT_FFOOD_SPEND,
      SUM(
        CASE
          WHEN MCC = 5541 THEN TRANS_AMT
          ELSE 0
          END) AS TOT_GAS_SPEND,
      SUM(
        CASE
          WHEN MCC = 5732 THEN TRANS_AMT
          ELSE 0
          END) AS TOT_ELCTRNCS_SPEND,
      SUM(
        CASE
          WHEN MCC = 4812 THEN TRANS_AMT
          ELSE 0
          END) AS TOT_TELCOSALE_SPEND,
      SUM(
        CASE
          WHEN MCC = 5651 THEN TRANS_AMT
          ELSE 0
          END) AS TOT_FAMCLOTHES_SPEND,
      SUM(
        CASE
          WHEN MCC = 5499 THEN TRANS_AMT
          ELSE 0
          END) AS TOT_CONV_SPEND,
      SUM(
        CASE
          WHEN MCC = 5200 THEN 1
          ELSE 0
          END) AS N_HARDWARE_TRNS,
      SUM(
        CASE
          WHEN MCC = 5541 THEN 1
          ELSE 0
          END) AS N_GAS_TRNS,
      SUM(
        CASE
          WHEN MCC = 5542 THEN 1
          ELSE 0
          END) AS N_FFOOD_TRNS,
      SUM(
        CASE
          WHEN MCC = 5812 THEN 1
          ELSE 0
          END) AS N_RESTO_TRNS,
      SUM(
        CASE
          WHEN MCC = 5912 THEN 1
          ELSE 0
          END) AS N_PHARM_TRNS,
      SUM(
        CASE
          WHEN MCC = 5921 THEN 1
          ELSE 0
          END) AS N_LICQ_TRNS,
      SUM(
        CASE
          WHEN MCC = 7523 THEN 1
          ELSE 0
          END) AS N_PARKING_TRNS,
      SUM(
        CASE
          WHEN MCC = 5732 THEN 1
          ELSE 0
          END) AS N_ELECTRNCS_TRNS,
      SUM(
        CASE
          WHEN MCC = 5331 THEN 1
          ELSE 0
          END) AS N_VARIETY_TRNS,
      SUM(
        CASE
          WHEN MCC = 5651 THEN 1
          ELSE 0
          END) AS N_FAMCLOTHES_TRNS,
      SUM(
        CASE
          WHEN MCC = 5499 THEN 1
          ELSE 0
          END) AS N_CONV_TRNS,
      SUM(
        CASE
          WHEN MCC = 3000 THEN 1
          ELSE 0
          END) AS N_AIRLINE_TRNS,
      SUM(
        CASE
          WHEN MCC = 5942 THEN 1
          ELSE 0
          END) AS N_BOOK_TRNS,
      SUM(
        CASE
          WHEN MCC = 5691 THEN 1
          ELSE 0
          END) AS N_CLOTHES_TRNS,
      SUM(
        CASE
          WHEN MCC = 5977 THEN 1
          ELSE 0
          END) AS N_COSMETIC_TRNS,
      SUM(
        CASE
          WHEN MCC = 5310 THEN 1
          ELSE 0
          END) AS N_DISCOUNT_TRNS,
      SUM(
        CASE
          WHEN MCC = 5712 THEN 1
          ELSE 0
          END) AS N_FURNITURE_TRNS,
      SUM(
        CASE
          WHEN MCC = 5945 THEN 1
          ELSE 0
          END) AS N_GAMES_TRNS,
      SUM(
        CASE
          WHEN MCC = 9399 THEN 1
          ELSE 0
          END) AS N_GOV_TRNS,
      SUM(
        CASE
          WHEN MCC IN (7011, 3500) THEN 1
          ELSE 0
          END) AS N_HOTEL_TRNS,
      SUM(
        CASE
          WHEN MCC = 6300 THEN 1
          ELSE 0
          END) AS N_INSURANCE_TRNS,
      SUM(
        CASE
          WHEN MCC = 4816 THEN 1
          ELSE 0
          END) AS N_ITSERVICE_TRNS,
      SUM(
        CASE
          WHEN MCC = 5999 THEN 1
          ELSE 0
          END) AS N_MISC_TRNS,
      SUM(
        CASE
          WHEN MCC = 5251 THEN 1
          ELSE 0
          END) AS N_RENO_TRNS,
      SUM(
        CASE
          WHEN MCC = 7230 THEN 1
          ELSE 0
          END) AS N_SALON_TRNS,
      SUM(
        CASE
          WHEN MCC = 5941 THEN 1
          ELSE 0
          END) AS N_SPORTG_TRNS,
      SUM(
        CASE
          WHEN MCC = 5621 THEN 1
          ELSE 0
          END) AS N_FRTWCLOTHES_TRNS,
      SUM(
        CASE
          WHEN MCC = 4121 THEN 1
          ELSE 0
          END) AS N_TAXI_TRNS,
    FROM base_data
    GROUP BY
      MAST_ACCOUNT_ID
  ),
  enriched_data_2 AS (
    SELECT DISTINCT
      *,
      ROUND((TOT_FFOOD_SPEND / NULLIF(TOT_SPEND, 0)), 2) AS PCT_FFOOD_SPEND,
      ROUND((N_GAS_TRNS / NULLIF(N_TOT_TRANS, 0)), 2) AS PCT_GAS_TRNS,
      ROUND((TOT_GAS_SPEND / NULLIF(TOT_SPEND, 0)), 2) AS PCT_GAS_SPEND,
      ROUND((N_RESTO_TRNS / NULLIF(N_TOT_TRANS, 0)), 2) AS PCT_RESTO_TRNS,
      ROUND((N_PHARM_TRNS / NULLIF(N_TOT_TRANS, 0)), 2) AS PCT_PHARM_TRNS,
      ROUND((N_LICQ_TRNS / NULLIF(N_TOT_TRANS, 0)), 2) AS PCT_LICQ_TRNS,
      ROUND((N_ELECTRNCS_TRNS / NULLIF(N_TOT_TRANS, 0)), 2)
        AS PCT_ELCTRNCS_TRNS,
      ROUND((TOT_ELCTRNCS_SPEND / NULLIF(TOT_SPEND, 0)), 2)
        AS PCT_ELCTRNCS_SPEND,
      ROUND((TOT_TELCOSALE_SPEND / NULLIF(TOT_SPEND, 0)), 2)
        AS PCT_TELCOSALE_SPEND,
      ROUND((TOT_FAMCLOTHES_SPEND / NULLIF(TOT_SPEND, 0)), 2)
        AS PCT_FAMCLOTHES_SPEND,
      ROUND((TOT_CONV_SPEND / NULLIF(TOT_SPEND, 0)), 2) AS PCT_CONV_SPEND,
      (
        CASE
          WHEN N_FFOOD_TRNS > 0 THEN 1
          ELSE 0
          END) AS FFOOD_IND,
      (
        CASE
          WHEN N_GAS_TRNS > 0 THEN 1
          ELSE 0
          END) AS GAS_IND,
      (
        CASE
          WHEN N_RESTO_TRNS > 0 THEN 1
          ELSE 0
          END) AS RESTO_IND,
      (
        CASE
          WHEN N_HARDWARE_TRNS > 0 THEN 1
          ELSE 0
          END) AS HARDWARE_IND,
      (
        CASE
          WHEN N_VARIETY_TRNS > 0 THEN 1
          ELSE 0
          END) AS VARIETY_IND,
      (
        CASE
          WHEN N_FAMCLOTHES_TRNS > 0 THEN 1
          ELSE 0
          END) AS FAMCLOTHES_IND,
      (
        CASE
          WHEN N_CONV_TRNS > 0 THEN 1
          ELSE 0
          END) AS CONV_IND,
      (
        CASE
          WHEN N_AIRLINE_TRNS > 0 THEN 1
          ELSE 0
          END) AS AIRLINE_IND,
      (
        CASE
          WHEN N_BOOK_TRNS > 0 THEN 1
          ELSE 0
          END) AS BOOK_IND,
      (
        CASE
          WHEN N_CLOTHES_TRNS > 0 THEN 1
          ELSE 0
          END) AS CLOTHES_IND,
      (
        CASE
          WHEN N_COSMETIC_TRNS > 0 THEN 1
          ELSE 0
          END) AS COSMETIC_IND,
      (
        CASE
          WHEN N_DISCOUNT_TRNS > 0 THEN 1
          ELSE 0
          END) AS DISCOUNT_IND,
      (
        CASE
          WHEN N_FURNITURE_TRNS > 0 THEN 1
          ELSE 0
          END) AS FURNITURE_IND,
      (
        CASE
          WHEN N_GAMES_TRNS > 0 THEN 1
          ELSE 0
          END) AS GAMES_IND,
      (
        CASE
          WHEN N_GOV_TRNS > 0 THEN 1
          ELSE 0
          END) AS GOV_IND,
      (
        CASE
          WHEN N_HOTEL_TRNS > 0 THEN 1
          ELSE 0
          END) AS HOTEL_IND,
      (
        CASE
          WHEN N_INSURANCE_TRNS > 0 THEN 1
          ELSE 0
          END) AS INSURANCE_IND,
      (
        CASE
          WHEN N_ITSERVICE_TRNS > 0 THEN 1
          ELSE 0
          END) AS ITSERVICE_IND,
      (
        CASE
          WHEN N_MISC_TRNS > 0 THEN 1
          ELSE 0
          END) AS MISC_IND,
      (
        CASE
          WHEN N_RENO_TRNS > 0 THEN 1
          ELSE 0
          END) AS RENO_IND,
      (
        CASE
          WHEN N_SALON_TRNS > 0 THEN 1
          ELSE 0
          END) AS SALON_IND,
      (
        CASE
          WHEN N_SPORTG_TRNS > 0 THEN 1
          ELSE 0
          END) AS SPORTG_IND,
      (
        CASE
          WHEN N_FRTWCLOTHES_TRNS > 0 THEN 1
          ELSE 0
          END) AS FRTWCLOTHES_IND,
      (
        CASE
          WHEN N_TAXI_TRNS > 0 THEN 1
          ELSE 0
          END) AS TAXI_IND
    FROM
      enriched_data_1
  )
SELECT DISTINCT
  CAST(enriched_data_2.MAST_ACCOUNT_ID AS STRING),
  CAST(enriched_data_2.N_TOT_TRANS AS STRING),
  CAST(enriched_data_2.TOT_SPEND AS STRING),
  CAST(enriched_data_2.TOT_FFOOD_SPEND AS STRING),
  CAST(enriched_data_2.TOT_GAS_SPEND AS STRING),
  CAST(enriched_data_2.TOT_ELCTRNCS_SPEND AS STRING),
  CAST(enriched_data_2.TOT_TELCOSALE_SPEND AS STRING),
  CAST(enriched_data_2.TOT_FAMCLOTHES_SPEND AS STRING),
  CAST(enriched_data_2.TOT_CONV_SPEND AS STRING),
  CAST(enriched_data_2.N_HARDWARE_TRNS AS STRING),
  CAST(enriched_data_2.N_GAS_TRNS AS STRING),
  CAST(enriched_data_2.N_FFOOD_TRNS AS STRING),
  CAST(enriched_data_2.N_RESTO_TRNS AS STRING),
  CAST(enriched_data_2.N_PHARM_TRNS AS STRING),
  CAST(enriched_data_2.N_LICQ_TRNS AS STRING),
  CAST(enriched_data_2.N_PARKING_TRNS AS STRING),
  CAST(enriched_data_2.N_ELECTRNCS_TRNS AS STRING),
  CAST(enriched_data_2.N_VARIETY_TRNS AS STRING),
  CAST(enriched_data_2.N_FAMCLOTHES_TRNS AS STRING),
  CAST(enriched_data_2.N_CONV_TRNS AS STRING),
  CAST(enriched_data_2.N_AIRLINE_TRNS AS STRING),
  CAST(enriched_data_2.N_BOOK_TRNS AS STRING),
  CAST(enriched_data_2.N_CLOTHES_TRNS AS STRING),
  CAST(enriched_data_2.N_COSMETIC_TRNS AS STRING),
  CAST(enriched_data_2.N_DISCOUNT_TRNS AS STRING),
  CAST(enriched_data_2.N_FURNITURE_TRNS AS STRING),
  CAST(enriched_data_2.N_GAMES_TRNS AS STRING),
  CAST(enriched_data_2.N_GOV_TRNS AS STRING),
  CAST(enriched_data_2.N_HOTEL_TRNS AS STRING),
  CAST(enriched_data_2.N_INSURANCE_TRNS AS STRING),
  CAST(enriched_data_2.N_ITSERVICE_TRNS AS STRING),
  CAST(enriched_data_2.N_MISC_TRNS AS STRING),
  CAST(enriched_data_2.N_RENO_TRNS AS STRING),
  CAST(enriched_data_2.N_SALON_TRNS AS STRING),
  CAST(enriched_data_2.N_SPORTG_TRNS AS STRING),
  CAST(enriched_data_2.N_FRTWCLOTHES_TRNS AS STRING),
  CAST(enriched_data_2.N_TAXI_TRNS AS STRING),
  CAST(enriched_data_2.PCT_FFOOD_SPEND AS STRING),
  CAST(enriched_data_2.PCT_GAS_TRNS AS STRING),
  CAST(enriched_data_2.PCT_GAS_SPEND AS STRING),
  CAST(enriched_data_2.PCT_RESTO_TRNS AS STRING),
  CAST(enriched_data_2.PCT_PHARM_TRNS AS STRING),
  CAST(enriched_data_2.PCT_LICQ_TRNS AS STRING),
  CAST(enriched_data_2.PCT_ELCTRNCS_TRNS AS STRING),
  CAST(enriched_data_2.PCT_ELCTRNCS_SPEND AS STRING),
  CAST(enriched_data_2.PCT_TELCOSALE_SPEND AS STRING),
  CAST(enriched_data_2.PCT_FAMCLOTHES_SPEND AS STRING),
  CAST(enriched_data_2.PCT_CONV_SPEND AS STRING),
  CAST(enriched_data_2.FFOOD_IND AS STRING),
  CAST(enriched_data_2.GAS_IND AS STRING),
  CAST(enriched_data_2.RESTO_IND AS STRING),
  CAST(enriched_data_2.HARDWARE_IND AS STRING),
  CAST(enriched_data_2.VARIETY_IND AS STRING),
  CAST(enriched_data_2.FAMCLOTHES_IND AS STRING),
  CAST(enriched_data_2.CONV_IND AS STRING),
  CAST(enriched_data_2.AIRLINE_IND AS STRING),
  CAST(enriched_data_2.BOOK_IND AS STRING),
  CAST(enriched_data_2.CLOTHES_IND AS STRING),
  CAST(enriched_data_2.COSMETIC_IND AS STRING),
  CAST(enriched_data_2.DISCOUNT_IND AS STRING),
  CAST(enriched_data_2.FURNITURE_IND AS STRING),
  CAST(enriched_data_2.GAMES_IND AS STRING),
  CAST(enriched_data_2.GOV_IND AS STRING),
  CAST(enriched_data_2.HOTEL_IND AS STRING),
  CAST(enriched_data_2.INSURANCE_IND AS STRING),
  CAST(enriched_data_2.ITSERVICE_IND AS STRING),
  CAST(enriched_data_2.MISC_IND AS STRING),
  CAST(enriched_data_2.RENO_IND AS STRING),
  CAST(enriched_data_2.SALON_IND AS STRING),
  CAST(enriched_data_2.SPORTG_IND AS STRING),
  CAST(enriched_data_2.FRTWCLOTHES_IND AS STRING),
  CAST(enriched_data_2.TAXI_IND AS STRING),
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM enriched_data_2;
