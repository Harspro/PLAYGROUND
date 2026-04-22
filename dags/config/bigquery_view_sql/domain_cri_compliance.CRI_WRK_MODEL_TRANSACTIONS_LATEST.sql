WITH
  cri_wrk_model_transactions_latest_load AS (
    SELECT
      MAX(cri_wrk_model_transactions.RecLoadTimestamp)
        AS LatestRecLoadTimestamp
    FROM
      `pcb-{env}-landing.domain_cri_compliance.CRI_WRK_MODEL_TRANSACTIONS`
        AS cri_wrk_model_transactions
  )
SELECT
  * EXCEPT (LatestRecLoadTimestamp)
FROM
  `pcb-{env}-landing.domain_cri_compliance.CRI_WRK_MODEL_TRANSACTIONS`
    AS cri_wrk_model_transactions
INNER JOIN
  cri_wrk_model_transactions_latest_load AS cri_wrk_model_transactions_ll
  ON
    cri_wrk_model_transactions.RecLoadTimestamp
    = cri_wrk_model_transactions_ll.LatestRecLoadTimestamp;