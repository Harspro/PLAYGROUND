CREATE OR REPLACE VIEW
  pcb-{env}-processing.domain_account_management.ACCOUNT_STATUS
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS
SELECT
  cifp_account_id5,
  customer_uid,
  UPPER(flag_name) AS flag_name
FROM pcb-{env}-processing.domain_account_management.BAD_STANDING_ACCOUNT_STATUS
UNPIVOT
  (flag_value FOR flag_name IN ( closed,
      chargeoff,
      credit_revoked,
      security_fraud,
      statc_watch,
      potential_purge))
JOIN
  pcb-{env}-processing.domain_account_management.CUSTOMER_ROLES
ON
  CUSTOMER_ROLES.account_no=cifp_account_id5
WHERE
  flag_value='TRUE';