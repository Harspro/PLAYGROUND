WITH
  cri_wrk_lookup_tcat_tcode_classification_latest_load AS (
    SELECT
      MAX(cri_wrk_lookup_tcat_tcode_classification.RecLoadTimestamp) AS LatestRecLoadTimestamp
    FROM
      `pcb-{env}-landing.domain_cri_compliance.CRI_WRK_LOOKUP_TCAT_TCODE_CLASSIFICATION` AS cri_wrk_lookup_tcat_tcode_classification
  )
SELECT * EXCEPT(LatestRecLoadTimestamp)
FROM
  `pcb-{env}-landing.domain_cri_compliance.CRI_WRK_LOOKUP_TCAT_TCODE_CLASSIFICATION` AS cri_wrk_lookup_tcat_tcode_classification
INNER JOIN
  cri_wrk_lookup_tcat_tcode_classification_latest_load AS cri_wrk_lookup_tcat_tcode_classification_ll
  ON
    cri_wrk_lookup_tcat_tcode_classification.RecLoadTimestamp = cri_wrk_lookup_tcat_tcode_classification_ll.LatestRecLoadTimestamp;