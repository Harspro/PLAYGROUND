  WITH
  scoring_prep_grocery_trans_daily_latest_load AS (
  SELECT
    MAX(SCORING_PREP_GROCERY_TRANS_DAILY.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-landing.domain_scoring.SCORING_PREP_GROCERY_TRANS_DAILY` AS scoring_prep_grocery_trans_daily )
SELECT
  * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_GROCERY_TRANS_DAILY` AS scoring_prep_grocery_trans_daily
INNER JOIN
  scoring_prep_grocery_trans_daily_latest_load AS scoring_prep_grocery_trans_daily_ll
ON
  scoring_prep_grocery_trans_daily.REC_LOAD_TIMESTAMP = scoring_prep_grocery_trans_daily_ll.LATEST_REC_LOAD_TIMESTAMP;