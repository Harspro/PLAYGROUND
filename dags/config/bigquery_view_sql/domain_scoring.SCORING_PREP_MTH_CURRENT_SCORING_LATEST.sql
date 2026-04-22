WITH
  scoring_prep_mth_current_scoring_latest_load AS (
    SELECT
      MAX(scoring_prep_mth_current_scoring.REC_LOAD_TIMESTAMP)
        AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.SCORING_PREP_MTH_CURRENT_SCORING`
        AS scoring_prep_mth_current_scoring
  )
SELECT * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_MTH_CURRENT_SCORING`
    AS scoring_prep_mth_current_scoring
INNER JOIN
  scoring_prep_mth_current_scoring_latest_load AS scoring_prep_mth_current_scoring_ll
  ON
    scoring_prep_mth_current_scoring.REC_LOAD_TIMESTAMP
    = scoring_prep_mth_current_scoring_ll.LATEST_REC_LOAD_TIMESTAMP;