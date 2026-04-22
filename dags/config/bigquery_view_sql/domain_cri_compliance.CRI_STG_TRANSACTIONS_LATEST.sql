WITH
  cri_stg_transactions_latest_load AS (
    SELECT
      MAX(cri_stg_transactions.RecLoadTimestamp)
        AS LatestRecLoadTimestamp
    FROM
      `pcb-{env}-landing.domain_cri_compliance.CRI_STG_TRANSACTIONS`
        AS cri_stg_transactions
  )
SELECT
  * EXCEPT (LatestRecLoadTimestamp)
FROM
  `pcb-{env}-landing.domain_cri_compliance.CRI_STG_TRANSACTIONS`
    AS cri_stg_transactions
INNER JOIN
  cri_stg_transactions_latest_load AS cri_stg_transactions_ll
  ON
    cri_stg_transactions.RecLoadTimestamp
    = cri_stg_transactions_ll.LatestRecLoadTimestamp;