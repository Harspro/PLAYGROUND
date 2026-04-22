WITH
  scoring_prep_cred_das_history_base_04_latest_load AS (
  SELECT
    MAX(scoring_prep_cred_das_history_base_04.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_04` AS scoring_prep_cred_das_history_base_04 )
SELECT
  * EXCEPT (LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_04` AS scoring_prep_cred_das_history_base_04
INNER JOIN
  scoring_prep_cred_das_history_base_04_latest_load AS scoring_prep_cred_das_history_base_04_ll
ON
  scoring_prep_cred_das_history_base_04.REC_LOAD_TIMESTAMP = scoring_prep_cred_das_history_base_04_ll.LATEST_REC_LOAD_TIMESTAMP;
