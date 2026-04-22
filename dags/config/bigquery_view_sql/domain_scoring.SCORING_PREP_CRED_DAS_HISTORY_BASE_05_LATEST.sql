WITH
  scoring_prep_cred_das_history_base_05_latest_load AS (
  SELECT
    MAX(scoring_prep_cred_das_history_base_05.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_05` AS scoring_prep_cred_das_history_base_05 )
SELECT
  * EXCEPT (LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_05` AS scoring_prep_cred_das_history_base_05
INNER JOIN
  scoring_prep_cred_das_history_base_05_latest_load AS scoring_prep_cred_das_history_base_05_ll
ON
  scoring_prep_cred_das_history_base_05.REC_LOAD_TIMESTAMP = scoring_prep_cred_das_history_base_05_ll.LATEST_REC_LOAD_TIMESTAMP;
