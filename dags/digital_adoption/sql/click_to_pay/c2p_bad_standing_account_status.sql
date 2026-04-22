CREATE OR REPLACE VIEW
  pcb-{env}-processing.domain_account_management.BAD_STANDING_ACCOUNT_STATUS
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS
SELECT
  CAST(cifp_account_id5 AS INT64) AS cifp_account_id5,
  CASE
    WHEN (am00_statc_closed IS NULL OR am00_statc_closed = '') THEN 'FALSE'
    ELSE 'TRUE'
END
  AS closed,
  CASE
    WHEN (am00_statc_chargeoff IS NULL OR am00_statc_chargeoff = '') THEN 'FALSE'
    ELSE 'TRUE'
END
  AS chargeoff,
  CASE
    WHEN (am00_statc_credit_revoked IS NULL OR am00_statc_credit_revoked = '') THEN 'FALSE'
    ELSE 'TRUE'
END
  AS credit_revoked,
  CASE
    WHEN (am00_statc_security_fraud IS NULL OR am00_statc_security_fraud = '') THEN 'FALSE'
    ELSE 'TRUE'
END
  AS security_fraud,
  CASE
    WHEN (COALESCE(am00_statc_watch,'') != 'SF') THEN 'FALSE'
    ELSE 'TRUE'
END
  AS statc_watch,
  CASE
    WHEN (am00_statf_potential_purge IS NULL OR am00_statf_potential_purge = '') THEN 'FALSE'
    ELSE 'TRUE'
END
  AS potential_purge
FROM
  `pcb-{env}-curated.domain_account_management.CIF_ACCOUNT_CURR`;
