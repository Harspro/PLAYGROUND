WITH
  scoring_prep_wk_tsys_chgoff_suffix_latest_load AS (
    SELECT
      MAX(scoring_prep_wk_tsys_chgoff_suffix.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_TSYS_CHGOFF_SUFFIX` AS scoring_prep_wk_tsys_chgoff_suffix
  )
SELECT * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_TSYS_CHGOFF_SUFFIX` AS scoring_prep_wk_tsys_chgoff_suffix
INNER JOIN
  scoring_prep_wk_tsys_chgoff_suffix_latest_load AS scoring_prep_wk_tsys_chgoff_suffix_ll
  ON
    scoring_prep_wk_tsys_chgoff_suffix.REC_LOAD_TIMESTAMP = scoring_prep_wk_tsys_chgoff_suffix_ll.LATEST_REC_LOAD_TIMESTAMP;