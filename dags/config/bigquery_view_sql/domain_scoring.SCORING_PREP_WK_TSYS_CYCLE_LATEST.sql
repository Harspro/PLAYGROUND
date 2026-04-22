WITH
  scoring_prep_wk_tsys_cycle_latest_load AS (
    SELECT
      MAX(scoring_prep_wk_tsys_cycle.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_TSYS_CYCLE` AS scoring_prep_wk_tsys_cycle
  )
SELECT * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_TSYS_CYCLE` AS scoring_prep_wk_tsys_cycle
INNER JOIN
  scoring_prep_wk_tsys_cycle_latest_load AS scoring_prep_wk_tsys_cycle_ll
  ON
    scoring_prep_wk_tsys_cycle.REC_LOAD_TIMESTAMP = scoring_prep_wk_tsys_cycle_ll.LATEST_REC_LOAD_TIMESTAMP;