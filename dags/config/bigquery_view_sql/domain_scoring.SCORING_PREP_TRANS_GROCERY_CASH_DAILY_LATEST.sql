WITH
  scoring_prep_trans_grocery_cash_daily_latest_load AS (
    SELECT
      MAX(scoring_prep_trans_grocery_cash_daily.REC_LOAD_TIMESTAMP)
        AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.SCORING_PREP_TRANS_GROCERY_CASH_DAILY`
        AS scoring_prep_trans_grocery_cash_daily
  )
SELECT * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_TRANS_GROCERY_CASH_DAILY`
    AS scoring_prep_trans_grocery_cash_daily
INNER JOIN
  scoring_prep_trans_grocery_cash_daily_latest_load AS scoring_prep_trans_grocery_cash_daily_ll
  ON
    scoring_prep_trans_grocery_cash_daily.REC_LOAD_TIMESTAMP
    = scoring_prep_trans_grocery_cash_daily_ll.LATEST_REC_LOAD_TIMESTAMP;