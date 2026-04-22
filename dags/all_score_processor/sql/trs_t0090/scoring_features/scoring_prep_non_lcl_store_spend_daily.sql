INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_NON_LCL_STORE_SPEND_DAILY`
(
  MAST_ACCOUNT_ID,
  TOB,
  TOT_NON_LCL_SPEND,
  N_NON_LCL_TRANS,
  AVG_NON_LCL_SPEND_PER_TRANS,
  AVG_NON_LCL_SPEND_PER_MONTH,
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
WITH
  cust_trans_numeric AS (
    SELECT DISTINCT
      cust_trans_daily.MAST_ACCOUNT_ID,
      CAST(cust_trans_daily.TOB AS INT64) AS TOB,
      CAST(cust_trans_daily.TRANS_AMT AS NUMERIC) AS TRANS_AMT
    FROM
      `pcb-{env}-curated.domain_scoring.SCORING_PREP_CUST_TRANS_FULL_DAILY_LATEST`
        AS cust_trans_daily
    LEFT JOIN
      `pcb-{env}-curated.domain_scoring.SCORING_PREP_LCL_STORE_LIST_LATEST`
        AS lcl_store_list
      ON cust_trans_daily.MERCHANT_ID = lcl_store_list.MERCHANT
    WHERE
      lcl_store_list.MERCHANT IS NULL
  ),
  non_lcl_metrics_by_account AS (
    SELECT DISTINCT
      trans_numeric.MAST_ACCOUNT_ID,
      MAX(trans_numeric.TOB) AS TOB,
      SUM(trans_numeric.TRANS_AMT) AS TOT_NON_LCL_SPEND,
      COUNT(trans_numeric.TRANS_AMT) AS N_NON_LCL_TRANS,
      ROUND(
        SUM(trans_numeric.TRANS_AMT)
          / NULLIF(COUNT(trans_numeric.TRANS_AMT), 0),
        2) AS AVG_NON_LCL_SPEND_PER_TRANS,
      ROUND(
        SUM(trans_numeric.TRANS_AMT)
          / NULLIF(MAX(trans_numeric.TOB), 0),
        2) AS AVG_NON_LCL_SPEND_PER_MONTH
    FROM
      cust_trans_numeric AS trans_numeric
    GROUP BY
      trans_numeric.MAST_ACCOUNT_ID
  )
SELECT DISTINCT
  non_lcl_metrics.MAST_ACCOUNT_ID,
  CAST(non_lcl_metrics.TOB AS STRING) AS TOB,
  CAST(non_lcl_metrics.TOT_NON_LCL_SPEND AS STRING) AS TOT_NON_LCL_SPEND,
  CAST(non_lcl_metrics.N_NON_LCL_TRANS AS STRING) AS N_NON_LCL_TRANS,
  CAST(non_lcl_metrics.AVG_NON_LCL_SPEND_PER_TRANS AS STRING)
    AS AVG_NON_LCL_SPEND_PER_TRANS,
  CAST(non_lcl_metrics.AVG_NON_LCL_SPEND_PER_MONTH AS STRING)
    AS AVG_NON_LCL_SPEND_PER_MONTH,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  non_lcl_metrics_by_account AS non_lcl_metrics;