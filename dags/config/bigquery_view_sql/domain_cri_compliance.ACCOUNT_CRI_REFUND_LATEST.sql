WITH
  account_cri_refund_latest_load AS (
    SELECT
      MAX(account_cri_refund.RecLoadTimestamp) AS LatestRecLoadTimestamp
    FROM
      `pcb-{env}-landing.domain_cri_compliance.ACCOUNT_CRI_REFUND`
        AS account_cri_refund
  )
SELECT
  * EXCEPT (LatestRecLoadTimestamp)
FROM
  `pcb-{env}-landing.domain_cri_compliance.ACCOUNT_CRI_REFUND`
    AS account_cri_refund
INNER JOIN
  account_cri_refund_latest_load AS account_cri_refund_ll
  ON
    account_cri_refund.RecLoadTimestamp
    = account_cri_refund_ll.LatestRecLoadTimestamp;