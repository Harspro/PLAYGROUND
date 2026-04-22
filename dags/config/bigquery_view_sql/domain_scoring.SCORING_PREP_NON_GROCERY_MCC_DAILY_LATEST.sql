WITH
  non_grocery_mcc_daily_latest_load AS (
    SELECT
      MAX(non_grocery_mcc_daily.REC_LOAD_TIMESTAMP)
        AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.SCORING_PREP_NON_GROCERY_MCC_DAILY`
        AS non_grocery_mcc_daily
  )
SELECT * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_NON_GROCERY_MCC_DAILY`
    AS non_grocery_mcc_daily
INNER JOIN
  non_grocery_mcc_daily_latest_load AS non_grocery_mcc_daily_ll
  ON
    non_grocery_mcc_daily.REC_LOAD_TIMESTAMP
    = non_grocery_mcc_daily_ll.LATEST_REC_LOAD_TIMESTAMP;