WITH
  scoring_prep_wk_tsys_mthend_full_latest_load AS (
  SELECT
    MAX(scoring_prep_wk_tsys_mthend_full.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_TSYS_MTHEND_FULL` AS scoring_prep_wk_tsys_mthend_full )
SELECT
  * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_TSYS_MTHEND_FULL` AS scoring_prep_wk_tsys_mthend_full
INNER JOIN
  scoring_prep_wk_tsys_mthend_full_latest_load AS scoring_prep_wk_tsys_mthend_full_ll
ON
  scoring_prep_wk_tsys_mthend_full.REC_LOAD_TIMESTAMP = scoring_prep_wk_tsys_mthend_full_ll.LATEST_REC_LOAD_TIMESTAMP;