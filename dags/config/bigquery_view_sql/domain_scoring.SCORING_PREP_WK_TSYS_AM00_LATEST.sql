WITH
  scoring_prep_wk_tsys_am00_latest_load AS (
  SELECT
    MAX(scoring_prep_wk_tsys_am00.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_TSYS_AM00` AS scoring_prep_wk_tsys_am00 )
SELECT
  * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_TSYS_AM00` AS scoring_prep_wk_tsys_am00
INNER JOIN
  scoring_prep_wk_tsys_am00_latest_load AS scoring_prep_wk_tsys_am00_ll
ON
  scoring_prep_wk_tsys_am00.REC_LOAD_TIMESTAMP = scoring_prep_wk_tsys_am00_ll.LATEST_REC_LOAD_TIMESTAMP;