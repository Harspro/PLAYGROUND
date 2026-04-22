WITH
  scoring_prep_trans_hist_base_latest_load AS (
    SELECT
      MAX(scoring_prep_trans_hist_base.REC_LOAD_TIMESTAMP)
        AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.SCORING_PREP_TRANS_HIST_BASE` AS scoring_prep_trans_hist_base
  )
SELECT * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_TRANS_HIST_BASE` AS scoring_prep_trans_hist_base
INNER JOIN
  scoring_prep_trans_hist_base_latest_load AS scoring_prep_trans_hist_base_ll
  ON
    scoring_prep_trans_hist_base.REC_LOAD_TIMESTAMP = scoring_prep_trans_hist_base_ll.LATEST_REC_LOAD_TIMESTAMP;
