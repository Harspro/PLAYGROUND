INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_LCL_STORE_SPEND_DAILY`
(
  MAST_ACCOUNT_ID,
  TOB,
  TOT_LCL_SPEND,
  N_LCL_TRANS,
  AVG_LCL_SPEND_PER_TRANS,
  AVG_LCL_SPEND_PER_MONTH,
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
SELECT DISTINCT
  cust_trans_daily.MAST_ACCOUNT_ID,
  CAST(MAX(CAST(cust_trans_daily.TOB AS INT64)) AS STRING) AS TOB,
  CAST(SUM(CAST(cust_trans_daily.TRANS_AMT AS NUMERIC)) AS STRING)
    AS TOT_LCL_SPEND,
  CAST(
    COUNT(
      CASE
        WHEN cust_trans_daily.MERCHANT_ID = lcl_store_list.MERCHANT THEN 1
        ELSE 0
        END)
    AS STRING) AS N_LCL_TRANS,
  CAST(
    ROUND(
      SUM(CAST(cust_trans_daily.TRANS_AMT AS NUMERIC))
        / NULLIF(
          COUNT(
            CASE
              WHEN cust_trans_daily.MERCHANT_ID = lcl_store_list.MERCHANT THEN 1
              ELSE 0
              END),
          0),
      2)
    AS STRING) AS AVG_LCL_SPEND_PER_TRANS,
  CAST(
    ROUND(
      SUM(CAST(cust_trans_daily.TRANS_AMT AS NUMERIC))
        / NULLIF(MAX(CAST(cust_trans_daily.TOB AS INT64)), 0),
      2)
    AS STRING)
    AS AVG_LCL_SPEND_PER_MONTH,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_scoring.SCORING_PREP_CUST_TRANS_FULL_DAILY_LATEST`
    cust_trans_daily
INNER JOIN
  `pcb-{env}-curated.domain_scoring.SCORING_PREP_LCL_STORE_LIST_LATEST`
    lcl_store_list
  ON cust_trans_daily.merchant_id = lcl_store_list.merchant
GROUP BY
  cust_trans_daily.mast_account_id;
