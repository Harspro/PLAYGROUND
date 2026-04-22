WITH
  scoring_prep_wk_hist_6mth_latest_load AS (
    SELECT
      MAX(scoring_prep_wk_hist_6mth.REC_LOAD_TIMESTAMP)
        AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_HIST_6MTH`
        AS scoring_prep_wk_hist_6mth
  )
SELECT * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_HIST_6MTH`
    AS scoring_prep_wk_hist_6mth
INNER JOIN
  scoring_prep_wk_hist_6mth_latest_load AS scoring_prep_wk_hist_6mth_ll
  ON
    scoring_prep_wk_hist_6mth.REC_LOAD_TIMESTAMP
    = scoring_prep_wk_hist_6mth_ll.LATEST_REC_LOAD_TIMESTAMP;