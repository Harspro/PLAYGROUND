WITH
  cri_audit_new_transaction_types_latest_load AS (
    SELECT
      MAX(cri_audit_new_transaction_types.RecLoadTimestamp) AS LatestRecLoadTimestamp
    FROM
      `pcb-{env}-landing.domain_cri_compliance.CRI_AUDIT_NEW_TRANSACTION_TYPES`
        AS cri_audit_new_transaction_types
  )
SELECT
  * EXCEPT (LatestRecLoadTimestamp)
FROM
  `pcb-{env}-landing.domain_cri_compliance.CRI_AUDIT_NEW_TRANSACTION_TYPES`
    AS cri_audit_new_transaction_types
INNER JOIN
  cri_audit_new_transaction_types_latest_load AS cri_audit_new_transaction_types_ll
  ON
    cri_audit_new_transaction_types.RecLoadTimestamp
    = cri_audit_new_transaction_types_ll.LatestRecLoadTimestamp;