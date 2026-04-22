WITH
  scoring_prep_account_ids_randomized_latest_load AS (
    SELECT
      MAX(scoring_prep_account_ids_randomized.REC_LOAD_TIMESTAMP)
        AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.SCORING_PREP_ACCOUNT_IDS_RANDOMIZED`
        AS scoring_prep_account_ids_randomized
  )
SELECT * EXCEPT (LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_ACCOUNT_IDS_RANDOMIZED`
    AS scoring_prep_account_ids_randomized
INNER JOIN
  scoring_prep_account_ids_randomized_latest_load AS scoring_prep_account_ids_randomized_ll
  ON
    scoring_prep_account_ids_randomized.REC_LOAD_TIMESTAMP
    = scoring_prep_account_ids_randomized_ll.LATEST_REC_LOAD_TIMESTAMP;