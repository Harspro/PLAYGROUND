WITH
  scoring_prep_wk_score_lnk_latest_load AS (
  SELECT
    MAX(scoring_prep_wk_score_lnk.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_SCORE_LNK` AS scoring_prep_wk_score_lnk )
SELECT * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_SCORE_LNK` AS scoring_prep_wk_score_lnk
INNER JOIN
  scoring_prep_wk_score_lnk_latest_load AS scoring_prep_wk_score_lnk_ll
ON
  scoring_prep_wk_score_lnk.REC_LOAD_TIMESTAMP = scoring_prep_wk_score_lnk_ll.LATEST_REC_LOAD_TIMESTAMP;