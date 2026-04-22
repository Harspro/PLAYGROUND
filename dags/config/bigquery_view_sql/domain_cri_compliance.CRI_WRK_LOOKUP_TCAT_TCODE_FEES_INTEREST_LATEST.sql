WITH
  cri_wrk_lookup_tcat_tcode_fees_interest_latest_load AS (
    SELECT
      MAX(cri_wrk_lookup_tcat_tcode_fees_interest.RecLoadTimestamp)
        AS LatestRecLoadTimestamp
    FROM
      `pcb-{env}-landing.domain_cri_compliance.CRI_WRK_LOOKUP_TCAT_TCODE_FEES_INTEREST`
        AS cri_wrk_lookup_tcat_tcode_fees_interest
  )
SELECT
  * EXCEPT (LatestRecLoadTimestamp)
FROM
  `pcb-{env}-landing.domain_cri_compliance.CRI_WRK_LOOKUP_TCAT_TCODE_FEES_INTEREST`
    AS cri_wrk_lookup_tcat_tcode_fees_interest
INNER JOIN
  cri_wrk_lookup_tcat_tcode_fees_interest_latest_load AS cri_wrk_lookup_tcat_tcode_fees_interest_ll
  ON
    cri_wrk_lookup_tcat_tcode_fees_interest.RecLoadTimestamp
    = cri_wrk_lookup_tcat_tcode_fees_interest_ll.LatestRecLoadTimestamp;