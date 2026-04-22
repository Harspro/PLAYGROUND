  WITH
  scoring_prep_demostats_latest_load AS (
  SELECT
    MAX(scoring_prep_demostats.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-landing.domain_scoring.SCORING_PREP_DEMOSTATS` AS scoring_prep_demostats )
SELECT
  * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_DEMOSTATS` AS scoring_prep_demostats
INNER JOIN
  scoring_prep_demostats_latest_load AS scoring_prep_demostats_ll
ON
  scoring_prep_demostats.REC_LOAD_TIMESTAMP = scoring_prep_demostats_ll.LATEST_REC_LOAD_TIMESTAMP;