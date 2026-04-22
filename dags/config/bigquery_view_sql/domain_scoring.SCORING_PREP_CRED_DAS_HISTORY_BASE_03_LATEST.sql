WITH
  scoring_prep_cred_das_history_base_03_latest_load AS (
  SELECT
    MAX(scoring_prep_cred_das_history_base_03.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_03` AS scoring_prep_cred_das_history_base_03 )
SELECT
  * EXCEPT (LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_CRED_DAS_HISTORY_BASE_03` AS scoring_prep_cred_das_history_base_03
INNER JOIN
  scoring_prep_cred_das_history_base_03_latest_load AS scoring_prep_cred_das_history_base_03_ll
ON
  scoring_prep_cred_das_history_base_03.REC_LOAD_TIMESTAMP = scoring_prep_cred_das_history_base_03_ll.LATEST_REC_LOAD_TIMESTAMP;
