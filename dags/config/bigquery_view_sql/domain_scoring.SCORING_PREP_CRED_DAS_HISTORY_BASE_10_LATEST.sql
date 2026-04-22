WITH
  scoring_prep_cred_das_history_base_10_latest_load AS (
  SELECT
    MAX(scoring_prep_cred_das_history_base_10.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_10` AS scoring_prep_cred_das_history_base_10 )
SELECT
  * EXCEPT (LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_10` AS scoring_prep_cred_das_history_base_10
INNER JOIN
  scoring_prep_cred_das_history_base_10_latest_load AS scoring_prep_cred_das_history_base_10_ll
ON
  scoring_prep_cred_das_history_base_10.REC_LOAD_TIMESTAMP = scoring_prep_cred_das_history_base_10_ll.LATEST_REC_LOAD_TIMESTAMP;
