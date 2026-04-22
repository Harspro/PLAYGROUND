INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_GROCERY_FOR_LCL_NONLCL_DAILY`
(
  MAST_ACCOUNT_ID,
  TOB,
  TOT_GROCERY_SPEND,
  TOT_GROCERY_TRANS,
  GROCERY_TRANS_PER_MONTH,
  GROCERY_SPEND_PER_MONTH,
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
WITH
  base_data AS (
    SELECT DISTINCT
      cust_trans_full_daily_latest.MAST_ACCOUNT_ID,
      CAST(cust_trans_full_daily_latest.TOB AS NUMERIC) AS TOB,
      CAST(cust_trans_full_daily_latest.MCC AS NUMERIC) AS MCC,
      CAST(cust_trans_full_daily_latest.TRANS_AMT AS NUMERIC) AS TRANS_AMT,
    FROM
      `pcb-{env}-curated.domain_scoring.SCORING_PREP_CUST_TRANS_FULL_DAILY_LATEST`
        AS cust_trans_full_daily_latest
  ),
  enriched_data_1 AS (
    SELECT DISTINCT
      MAST_ACCOUNT_ID,
      MAX(TOB) AS TOB,
      SUM(
        CASE
          WHEN MCC = 5411 THEN TRANS_AMT
          ELSE 0
          END) AS TOT_GROCERY_SPEND,
      SUM(
        CASE
          WHEN MCC = 5411 THEN 1
          ELSE 0
          END) AS TOT_GROCERY_TRANS,
      ROUND(
        SAFE_DIVIDE(
          SUM(
            CASE
              WHEN MCC = 5411 THEN 1
              ELSE 0
              END),
          MAX(TOB)),
        2) AS GROCERY_TRANS_PER_MONTH,
      ROUND(
        SAFE_DIVIDE(
          SUM(
            CASE
              WHEN MCC = 5411 THEN TRANS_AMT
              ELSE 0
              END),
          MAX(TOB)),
        2) AS GROCERY_SPEND_PER_MONTH
    FROM
      base_data
    GROUP BY
      MAST_ACCOUNT_ID
  )
SELECT DISTINCT
  CAST(MAST_ACCOUNT_ID AS STRING) AS MAST_ACCOUNT_ID,
  CAST(TOB AS STRING) AS TOB,
  CAST(TOT_GROCERY_SPEND AS STRING) AS TOT_GROCERY_SPEND,
  CAST(TOT_GROCERY_TRANS AS STRING) AS TOT_GROCERY_TRANS,
  CAST(GROCERY_TRANS_PER_MONTH AS STRING) AS GROCERY_TRANS_PER_MONTH,
  CAST(GROCERY_SPEND_PER_MONTH AS STRING) AS GROCERY_SPEND_PER_MONTH,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  enriched_data_1
