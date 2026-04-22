WITH
  scoring_prep_decision_keys_latest_load AS (
    SELECT
      MAX(scoring_prep_decision_keys.REC_LOAD_TIMESTAMP)
        AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.SCORING_PREP_DECISION_KEYS`
        AS scoring_prep_decision_keys
  )
SELECT * EXCEPT (LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_DECISION_KEYS`
    AS scoring_prep_decision_keys
INNER JOIN
  scoring_prep_decision_keys_latest_load AS scoring_prep_decision_keys_ll
  ON
    scoring_prep_decision_keys.REC_LOAD_TIMESTAMP
    = scoring_prep_decision_keys_ll.LATEST_REC_LOAD_TIMESTAMP;
