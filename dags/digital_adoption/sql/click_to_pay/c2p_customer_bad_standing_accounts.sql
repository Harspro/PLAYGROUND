CREATE OR REPLACE VIEW
  pcb-{env}-processing.domain_account_management.CUSTOMER_BAD_STANDING_ACCOUNTS
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS
SELECT
  DISTINCT CAST(cifp_account_id5 AS INT64) AS cifp_account_id5,
  ACTIVE_CUSTOMER_BASE.customer_uid
FROM
  pcb-{env}-curated.domain_account_management.CIF_ACCOUNT_CURR
JOIN
  pcb-{env}-processing.domain_account_management.ACTIVE_CUSTOMER_BASE
ON
  ACTIVE_CUSTOMER_BASE.account_no=CAST(cifp_account_id5 AS INT64)
WHERE
  (am00_statc_chargeoff IS NOT NULL
    AND am00_statc_chargeoff!='')
  OR (am00_statc_credit_revoked IS NOT NULL
    AND am00_statc_credit_revoked!='')
  OR (am00_statc_security_fraud IS NOT NULL
    AND am00_statc_security_fraud!='')
  OR (COALESCE(am00_statc_watch,'') = 'SF')
  OR (am00_statf_potential_purge IS NOT NULL
    AND am00_statf_potential_purge!='');