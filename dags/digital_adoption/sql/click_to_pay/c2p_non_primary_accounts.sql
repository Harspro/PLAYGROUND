CREATE OR REPLACE VIEW
  pcb-{env}-processing.domain_account_management.C2P_NON_PRIMARY_ACCOUNTS
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS
SELECT
  customer_uid,
  FALSE AS eligibility_flag,
  role_description AS ineligibility_reason
FROM
  pcb-{env}-processing.domain_account_management.CUSTOMER_ROLES
WHERE
  UPPER(role_description)!='PRIMARY-CARD-HOLDER';