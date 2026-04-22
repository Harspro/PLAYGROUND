WITH
  cri_wrk_lookup_tcat_tcode_master_latest_load AS (
    SELECT
      MAX(cri_wrk_lookup_tcat_tcode_master.RecLoadTimestamp) AS Latest_RecLoadTimestamp
    FROM
      `pcb-{env}-landing.domain_cri_compliance.CRI_WRK_LOOKUP_TCAT_TCODE_MASTER` AS cri_wrk_lookup_tcat_tcode_master
  )
SELECT * EXCEPT(Latest_RecLoadTimestamp)
FROM
  `pcb-{env}-landing.domain_cri_compliance.CRI_WRK_LOOKUP_TCAT_TCODE_MASTER` AS cri_wrk_lookup_tcat_tcode_master
INNER JOIN
  cri_wrk_lookup_tcat_tcode_master_latest_load AS cri_wrk_lookup_tcat_tcode_master_ll
  ON
    cri_wrk_lookup_tcat_tcode_master.RecLoadTimestamp = cri_wrk_lookup_tcat_tcode_master_ll.Latest_RecLoadTimestamp;