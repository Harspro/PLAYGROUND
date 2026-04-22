WITH
  scoring_prep_wk_ace_final_latest_load AS (
  SELECT
    MAX(scoring_prep_wk_ace_final.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_ACE_FINAL` AS scoring_prep_wk_ace_final )
SELECT
  * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_ACE_FINAL` AS scoring_prep_wk_ace_final
INNER JOIN
  scoring_prep_wk_ace_final_latest_load AS scoring_prep_wk_ace_final_ll
ON
  scoring_prep_wk_ace_final.REC_LOAD_TIMESTAMP = scoring_prep_wk_ace_final_ll.LATEST_REC_LOAD_TIMESTAMP;