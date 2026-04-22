CREATE OR REPLACE VIEW
  pcb-{env}-processing.domain_account_management.C2P_ABB_PCMA_ACCOUNTS
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS
SELECT
  CUSTOMER_ROLES.customer_uid,
  FALSE AS eligibility_flag,
  'PCMA_ABB' AS ineligibility_reason
FROM
  pcb-{env}-processing.domain_account_management.CUSTOMER_ROLES
INNER JOIN
  pcb-{env}-processing.domain_account_management.PCMA_ABB_ACCOUNTS
ON
  CUSTOMER_ROLES.customer_uid = PCMA_ABB_ACCOUNTS.customer_uid;