WITH
  account_cri_evaluation_latest_load AS (
    SELECT
      MAX(account_cri_evaluation.RecLoadTimestamp)
        AS LatestRecLoadTimestamp
    FROM
      `pcb-{env}-landing.domain_cri_compliance.ACCOUNT_CRI_EVALUATION`
        AS account_cri_evaluation
  )
SELECT
  * EXCEPT (LatestRecLoadTimestamp)
FROM
  `pcb-{env}-landing.domain_cri_compliance.ACCOUNT_CRI_EVALUATION`
    AS account_cri_evaluation
INNER JOIN
  account_cri_evaluation_latest_load AS account_cri_evaluation_ll
  ON
    account_cri_evaluation.RecLoadTimestamp
    = account_cri_evaluation_ll.LatestRecLoadTimestamp;