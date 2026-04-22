WITH
  lcl_nonlcl_grocery_daily_latest_load AS (
    SELECT
      MAX(lcl_nonlcl_grocery_daily.REC_LOAD_TIMESTAMP)
        AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.SCORING_PREP_LCL_NONLCL_GROCERY_DAILY`
        AS lcl_nonlcl_grocery_daily
  )
SELECT * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_LCL_NONLCL_GROCERY_DAILY`
    AS lcl_nonlcl_grocery_daily
INNER JOIN
  lcl_nonlcl_grocery_daily_latest_load AS lcl_nonlcl_grocery_daily_ll
  ON
    lcl_nonlcl_grocery_daily.REC_LOAD_TIMESTAMP
    = lcl_nonlcl_grocery_daily_ll.LATEST_REC_LOAD_TIMESTAMP;