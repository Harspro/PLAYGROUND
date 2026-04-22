WITH
  cri_wrk_account_master_latest_load AS (
    SELECT
      MAX(cri_wrk_account_master.RecLoadTimestamp) AS LatestRecLoadTimestamp
    FROM
      `pcb-{env}-landing.domain_cri_compliance.CRI_WRK_ACCOUNT_MASTER` AS cri_wrk_account_master
  )
SELECT * EXCEPT(LatestRecLoadTimestamp)
FROM
  `pcb-{env}-landing.domain_cri_compliance.CRI_WRK_ACCOUNT_MASTER` AS cri_wrk_account_master
INNER JOIN
  cri_wrk_account_master_latest_load AS cri_wrk_account_master_ll
  ON
    cri_wrk_account_master.RecLoadTimestamp = cri_wrk_account_master_ll.LatestRecLoadTimestamp;