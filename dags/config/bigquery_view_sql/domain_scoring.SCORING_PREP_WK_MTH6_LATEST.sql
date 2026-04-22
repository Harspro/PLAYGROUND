WITH
  scoring_prep_wk_mth6_latest_load AS (
    SELECT
      MAX(scoring_prep_wk_mth6.REC_LOAD_TIMESTAMP)
        AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_MTH6`
        AS scoring_prep_wk_mth6
  )
SELECT * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_MTH6`
    AS scoring_prep_wk_mth6
INNER JOIN
  scoring_prep_wk_mth6_latest_load AS scoring_prep_wk_mth6_ll
  ON
    scoring_prep_wk_mth6.REC_LOAD_TIMESTAMP
    = scoring_prep_wk_mth6_ll.LATEST_REC_LOAD_TIMESTAMP;