WITH
  scoring_prep_pco_score_latest_load AS (
    SELECT
      MAX(scoring_prep_pco_score.REC_LOAD_TIMESTAMP)
        AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.SCORING_PREP_PCO_SCORE`
        AS scoring_prep_pco_score
  )
SELECT * EXCEPT (LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_PCO_SCORE`
    AS scoring_prep_pco_score
INNER JOIN
  scoring_prep_pco_score_latest_load AS scoring_prep_pco_score_ll
  ON
    scoring_prep_pco_score.REC_LOAD_TIMESTAMP
    = scoring_prep_pco_score_ll.LATEST_REC_LOAD_TIMESTAMP;