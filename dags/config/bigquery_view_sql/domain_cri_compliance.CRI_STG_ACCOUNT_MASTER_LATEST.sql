WITH
  cri_stg_account_master_latest_load AS (
    SELECT
      MAX(cri_stg_account_master.RecLoadTimestamp) AS LatestRecLoadTimestamp
    FROM
      `pcb-{env}-landing.domain_cri_compliance.CRI_STG_ACCOUNT_MASTER`
        AS cri_stg_account_master
  )
SELECT
  * EXCEPT (LatestRecLoadTimestamp)
FROM
  `pcb-{env}-landing.domain_cri_compliance.CRI_STG_ACCOUNT_MASTER`
    AS cri_stg_account_master
INNER JOIN
  cri_stg_account_master_latest_load AS cri_stg_account_master_ll
  ON
    cri_stg_account_master.RecLoadTimestamp
    = cri_stg_account_master_ll.LatestRecLoadTimestamp;