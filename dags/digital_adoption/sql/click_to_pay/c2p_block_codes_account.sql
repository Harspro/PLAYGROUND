CREATE OR REPLACE VIEW
  pcb-{env}-processing.domain_account_management.C2P_BLOCK_CODES_ACCOUNT
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS
SELECT
  CUSTOMER_ROLES.customer_uid,
  FALSE AS eligibility_flag,
  cifp_block_code AS ineligibility_reason
FROM
  pcb-{env}-processing.domain_account_management.CUSTOMER_ROLES
INNER JOIN
  pcb-{env}-processing.domain_account_management.CUSTOMER_BLOCK_CODES
ON
  CUSTOMER_ROLES.customer_uid = CUSTOMER_BLOCK_CODES.customer_uid;