CREATE OR REPLACE VIEW
  pcb-{env}-processing.domain_account_management.C2P_EXCLUDED_CUSTOMERS
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS
SELECT
  ineligible.customer_uid,
  ineligible.eligibility_flag,
  ineligible.ineligibility_reason
FROM
  pcb-{env}-curated.domain_account_management.C2P_INELIGIBLE_CUSTOMERS ineligible
LEFT JOIN
  pcb-{env}-curated.domain_account_management.C2P_ELIGIBLE_CUSTOMERS eligible
ON ineligible.customer_uid = eligible.customer_uid
WHERE
  eligible.customer_uid IS NULL;