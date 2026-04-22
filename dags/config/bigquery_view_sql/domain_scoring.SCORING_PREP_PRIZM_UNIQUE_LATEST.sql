WITH
  scoring_prep_prizm_unique_latest_load AS (
  SELECT
    MAX(scoring_prep_prizm_unique.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-landing.domain_scoring.SCORING_PREP_PRIZM_UNIQUE` AS scoring_prep_prizm_unique )
SELECT
  * EXCEPT (LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_PRIZM_UNIQUE` AS scoring_prep_prizm_unique
INNER JOIN
  scoring_prep_prizm_unique_latest_load AS scoring_prep_prizm_unique_ll
ON
  scoring_prep_prizm_unique.REC_LOAD_TIMESTAMP = scoring_prep_prizm_unique_ll.LATEST_REC_LOAD_TIMESTAMP;