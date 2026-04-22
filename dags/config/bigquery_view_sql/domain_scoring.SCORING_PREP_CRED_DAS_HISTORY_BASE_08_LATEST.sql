WITH
  scoring_prep_cred_das_history_base_08_latest_load AS (
  SELECT
    MAX(scoring_prep_cred_das_history_base_08.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_08` AS scoring_prep_cred_das_history_base_08 )
SELECT
  * EXCEPT (LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_08` AS scoring_prep_cred_das_history_base_08
INNER JOIN
  scoring_prep_cred_das_history_base_08_latest_load AS scoring_prep_cred_das_history_base_08_ll
ON
  scoring_prep_cred_das_history_base_08.REC_LOAD_TIMESTAMP = scoring_prep_cred_das_history_base_08_ll.LATEST_REC_LOAD_TIMESTAMP;
