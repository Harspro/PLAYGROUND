WITH
  scoring_prep_wk_tsys_chgoff_fr_latest_load AS (
    SELECT
      MAX(scoring_prep_wk_tsys_chgoff_fr.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_TSYS_CHGOFF_FR` AS scoring_prep_wk_tsys_chgoff_fr
  )
SELECT * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_TSYS_CHGOFF_FR` AS scoring_prep_wk_tsys_chgoff_fr
INNER JOIN
  scoring_prep_wk_tsys_chgoff_fr_latest_load AS scoring_prep_wk_tsys_chgoff_fr_ll
  ON
    scoring_prep_wk_tsys_chgoff_fr.REC_LOAD_TIMESTAMP = scoring_prep_wk_tsys_chgoff_fr_ll.LATEST_REC_LOAD_TIMESTAMP;