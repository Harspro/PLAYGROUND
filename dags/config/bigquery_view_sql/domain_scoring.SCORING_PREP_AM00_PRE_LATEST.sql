WITH
  scoring_prep_am00_pre_latest_load AS (
    SELECT
      MAX(scoring_prep_am00_pre.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.SCORING_PREP_AM00_PRE` AS scoring_prep_am00_pre
  )
SELECT
    * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_AM00_PRE` AS scoring_prep_am00_pre
INNER JOIN
  scoring_prep_am00_pre_latest_load AS scoring_prep_am00_pre_ll
  ON
    scoring_prep_am00_pre.REC_LOAD_TIMESTAMP = scoring_prep_am00_pre_ll.LATEST_REC_LOAD_TIMESTAMP;