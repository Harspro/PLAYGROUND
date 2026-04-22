CREATE OR REPLACE VIEW
  pcb-{env}-processing.domain_account_management.UNIFIED_CUSTOMER_ELIGIBILITY
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS
SELECT
  customer_uid,
  eligibility_flag,
  ineligibility_reason
FROM
  pcb-{env}-processing.domain_account_management.C2P_EXCLUDED_CUSTOMERS
UNION ALL
SELECT
  customer_uid,
  TRUE AS eligibility_flag,
  NULL AS ineligibility_reason
FROM
  pcb-{env}-curated.domain_account_management.C2P_ELIGIBLE_CUSTOMERS;
