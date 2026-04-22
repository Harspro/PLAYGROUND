INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_GROCERY_TRANS_DAILY`
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
  cust_trans_numeric AS (
    SELECT DISTINCT
      cust_trans_daily.MAST_ACCOUNT_ID,
      CAST(cust_trans_daily.TOB AS INT64) AS TOB,
      CAST(cust_trans_daily.MCC AS NUMERIC) AS MCC,
      CAST(cust_trans_daily.TRANS_AMT AS NUMERIC) AS TRANS_AMT
    FROM
      `pcb-{env}-curated.domain_scoring.SCORING_PREP_CUST_TRANS_FULL_DAILY_LATEST`
        AS cust_trans_daily
  ),
  grocery_metrics_by_account AS (
    SELECT DISTINCT
      trans_numeric.MAST_ACCOUNT_ID,
      MAX(trans_numeric.TOB) AS TOB,
      SUM(
        CASE
          WHEN trans_numeric.MCC = 5411 THEN trans_numeric.TRANS_AMT
          ELSE 0
          END) AS TOT_GROCERY_SPEND,
      SUM(CASE WHEN trans_numeric.MCC = 5411 THEN 1 ELSE 0 END)
        AS TOT_GROCERY_TRANS,
      ROUND(
        SAFE_DIVIDE(
          SUM(CASE WHEN trans_numeric.MCC = 5411 THEN 1 ELSE 0 END),
          MAX(trans_numeric.TOB)),
        2) AS GROCERY_TRANS_PER_MONTH,
      ROUND(
        SAFE_DIVIDE(
          SUM(
            CASE
              WHEN trans_numeric.MCC = 5411 THEN trans_numeric.TRANS_AMT
              ELSE 0
              END),
          MAX(trans_numeric.TOB)),
        2) AS GROCERY_SPEND_PER_MONTH
    FROM
      cust_trans_numeric AS trans_numeric
    GROUP BY
      trans_numeric.MAST_ACCOUNT_ID
  )
SELECT DISTINCT
  grocery_metrics.MAST_ACCOUNT_ID,
  CAST(grocery_metrics.TOB AS STRING) AS TOB,
  CAST(grocery_metrics.TOT_GROCERY_SPEND AS STRING) AS TOT_GROCERY_SPEND,
  CAST(grocery_metrics.TOT_GROCERY_TRANS AS STRING) AS TOT_GROCERY_TRANS,
  CAST(grocery_metrics.GROCERY_TRANS_PER_MONTH AS STRING)
    AS GROCERY_TRANS_PER_MONTH,
  CAST(grocery_metrics.GROCERY_SPEND_PER_MONTH AS STRING)
    AS GROCERY_SPEND_PER_MONTH,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  grocery_metrics_by_account AS grocery_metrics;