WITH
  scoring_prep_wk_tsys_cycle_trans_latest_load AS (
    SELECT
      MAX(scoring_prep_wk_tsys_cycle_trans.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_TSYS_CYCLE_TRANS` AS scoring_prep_wk_tsys_cycle_trans
  )
SELECT * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_TSYS_CYCLE_TRANS` AS scoring_prep_wk_tsys_cycle_trans
INNER JOIN
  scoring_prep_wk_tsys_cycle_trans_latest_load AS scoring_prep_wk_tsys_cycle_trans_ll
  ON
    scoring_prep_wk_tsys_cycle_trans.REC_LOAD_TIMESTAMP = scoring_prep_wk_tsys_cycle_trans_ll.LATEST_REC_LOAD_TIMESTAMP;