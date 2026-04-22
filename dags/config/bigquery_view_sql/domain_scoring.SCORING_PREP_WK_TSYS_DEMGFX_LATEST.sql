WITH
  scoring_prep_wk_tsys_demgfx_latest_load AS (
    SELECT
      MAX(scoring_prep_wk_tsys_demgfx.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_TSYS_DEMGFX` AS scoring_prep_wk_tsys_demgfx
  )
SELECT * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_TSYS_DEMGFX` AS scoring_prep_wk_tsys_demgfx
INNER JOIN
  scoring_prep_wk_tsys_demgfx_latest_load AS scoring_prep_wk_tsys_demgfx_ll
  ON
    scoring_prep_wk_tsys_demgfx.REC_LOAD_TIMESTAMP = scoring_prep_wk_tsys_demgfx_ll.LATEST_REC_LOAD_TIMESTAMP;