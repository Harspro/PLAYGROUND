CREATE OR REPLACE VIEW
  pcb-{env}-processing.domain_account_management.GOOD_STANDING_ACCOUNTS
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS
SELECT
  DISTINCT CAST(cifp_account_id5 AS INT64) AS cifp_account_id5
FROM
  `pcb-{env}-curated.domain_account_management.CIF_ACCOUNT_CURR`
WHERE
  (am00_statc_closed IS NULL
    OR am00_statc_closed='')
  AND (am00_statc_chargeoff IS NULL
    OR am00_statc_chargeoff='')
  AND (am00_statc_credit_revoked IS NULL
    OR am00_statc_credit_revoked='')
  AND (am00_statc_security_fraud IS NULL
    OR am00_statc_security_fraud='')
  AND (COALESCE(am00_statc_watch,'') != 'SF')
  AND (am00_statf_potential_purge IS NULL
    OR am00_statf_potential_purge='');