WITH
  scoring_prep_wk_tsys_am01_latest_load AS (
  SELECT
    MAX(scoring_prep_wk_tsys_am01.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_TSYS_AM01` AS scoring_prep_wk_tsys_am01 )
SELECT
  * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_TSYS_AM01` AS scoring_prep_wk_tsys_am01
INNER JOIN
  scoring_prep_wk_tsys_am01_latest_load AS scoring_prep_wk_tsys_am01_ll
ON
  scoring_prep_wk_tsys_am01.REC_LOAD_TIMESTAMP = scoring_prep_wk_tsys_am01_ll.LATEST_REC_LOAD_TIMESTAMP;