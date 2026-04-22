CREATE OR REPLACE VIEW
  pcb-{env}-processing.domain_account_management.C2P_BAD_STANDING_ACCOUNTS
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS
SELECT
  CUSTOMER_ROLES.customer_uid,
  FALSE AS eligibility_flag,
  flag_name AS ineligibility_reason
FROM
  pcb-{env}-processing.domain_account_management.CUSTOMER_ROLES
INNER JOIN
  pcb-{env}-processing.domain_account_management.ACCOUNT_STATUS
ON
  CUSTOMER_ROLES.customer_uid = ACCOUNT_STATUS.customer_uid;
