WITH
  cri_audit_data_issues_latest_load AS (
    SELECT
      MAX(cri_audit_data_issues.RecLoadTimestamp) AS LatestRecLoadTimestamp
    FROM
      `pcb-{env}-landing.domain_cri_compliance.CRI_AUDIT_DATA_ISSUES`
        AS cri_audit_data_issues
  )
SELECT
  * EXCEPT (LatestRecLoadTimestamp)
FROM
  `pcb-{env}-landing.domain_cri_compliance.CRI_AUDIT_DATA_ISSUES`
    AS cri_audit_data_issues
INNER JOIN
  cri_audit_data_issues_latest_load AS cri_audit_data_issues_ll
  ON
    cri_audit_data_issues.RecLoadTimestamp
    = cri_audit_data_issues_ll.LatestRecLoadTimestamp;
    