INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_TRANS_GROCERY_CASH_DAILY`
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
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
WITH
  scoring_prep_cash_advances_daily AS (
    SELECT DISTINCT
      mast_account_id,
      COUNT(*) AS n_cash_advances,
      SUM(SAFE_CAST(trans_amt AS NUMERIC)) AS tot_cash_advances
    FROM
      `pcb-{env}-curated.domain_scoring.SCORING_PREP_CUST_TRANS_FULL_DAILY_LATEST`
    WHERE mcc = '6011'
    GROUP BY mast_account_id
  )
SELECT DISTINCT
  trans_summary.MAST_ACCOUNT_ID,
  trans_summary.TOB,
  trans_summary.OPEN_TOB,
  trans_summary.N_TOT_TRANS,
  trans_summary.TRNS_PER_MTH,
  trans_summary.TOT_SPEND,
  trans_summary.SPEND_PER_TRNS,
  grocery_trans.TOT_GROCERY_SPEND,
  grocery_trans.TOT_GROCERY_TRANS,
  grocery_trans.GROCERY_TRANS_PER_MONTH,
  grocery_trans.GROCERY_SPEND_PER_MONTH,
  CAST(cash_advances.n_cash_advances AS STRING) AS n_cash_advances,
  CAST(cash_advances.tot_cash_advances AS STRING) AS tot_cash_advances,
  CAST(round(SAFE_DIVIDE(cash_advances.tot_cash_advances, nullif(SAFE_CAST(trans_summary.tot_spend AS NUMERIC), 0)), 2) AS STRING) AS cash_pct_of_spend,
  CAST(round(SAFE_DIVIDE(cash_advances.n_cash_advances, nullif(SAFE_CAST(trans_summary.n_tot_trans AS NUMERIC), 0)), 2) AS STRING) AS cash_pct_of_trns,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_scoring.SCORING_PREP_TRANS_SUMMARY_DAILY_LATEST` trans_summary
LEFT JOIN
  `pcb-{env}-curated.domain_scoring.SCORING_PREP_GROCERY_TRANS_DAILY_LATEST` grocery_trans
  ON trans_summary.mast_account_id = grocery_trans.mast_account_id
LEFT JOIN
  scoring_prep_cash_advances_daily cash_advances
  ON grocery_trans.mast_account_id = cash_advances.mast_account_id;