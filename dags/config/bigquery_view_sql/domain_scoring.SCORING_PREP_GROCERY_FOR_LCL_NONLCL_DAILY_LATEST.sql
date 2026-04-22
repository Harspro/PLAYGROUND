WITH
  grocery_for_lcl_nonlcl_daily_latest_load AS (
    SELECT
      MAX(grocery_for_lcl_nonlcl_daily.REC_LOAD_TIMESTAMP)
        AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.SCORING_PREP_GROCERY_FOR_LCL_NONLCL_DAILY`
        AS grocery_for_lcl_nonlcl_daily
  )
SELECT * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_GROCERY_FOR_LCL_NONLCL_DAILY`
    AS grocery_for_lcl_nonlcl_daily
INNER JOIN
  grocery_for_lcl_nonlcl_daily_latest_load AS grocery_for_lcl_nonlcl_daily_ll
  ON
    grocery_for_lcl_nonlcl_daily.REC_LOAD_TIMESTAMP
    = grocery_for_lcl_nonlcl_daily_ll.LATEST_REC_LOAD_TIMESTAMP;