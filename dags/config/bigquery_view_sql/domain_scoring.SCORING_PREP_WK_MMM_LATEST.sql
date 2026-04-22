WITH
  scoring_prep_wk_mmm_latest_load AS (
    SELECT
      MAX(scoring_prep_wk_mmm.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_MMM` AS scoring_prep_wk_mmm
  )
SELECT * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_MMM` AS scoring_prep_wk_mmm
INNER JOIN
  scoring_prep_wk_mmm_latest_load AS scoring_prep_wk_mmm_ll
  ON
    scoring_prep_wk_mmm.REC_LOAD_TIMESTAMP = scoring_prep_wk_mmm_ll.LATEST_REC_LOAD_TIMESTAMP;