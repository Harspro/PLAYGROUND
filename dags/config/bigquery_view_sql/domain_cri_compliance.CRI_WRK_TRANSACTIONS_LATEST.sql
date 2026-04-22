WITH
  cri_wrk_transactions_latest_load AS (
    SELECT
      MAX(cri_wrk_transactions.RecLoadTimestamp) AS LatestRecLoadTimestamp
    FROM
      `pcb-{env}-landing.domain_cri_compliance.CRI_WRK_TRANSACTIONS`
        AS cri_wrk_transactions
  )
SELECT
  * EXCEPT (LatestRecLoadTimestamp)
FROM
  `pcb-{env}-landing.domain_cri_compliance.CRI_WRK_TRANSACTIONS`
    AS cri_wrk_transactions
INNER JOIN
  cri_wrk_transactions_latest_load AS cri_wrk_transactions_ll
  ON
    cri_wrk_transactions.RecLoadTimestamp
    = cri_wrk_transactions_ll.LatestRecLoadTimestamp;