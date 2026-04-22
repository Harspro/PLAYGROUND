CREATE OR REPLACE VIEW
  pcb-{env}-processing.domain_account_management.C2P_NO_EMAIL_ACCOUNTS
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS
SELECT
  customer_uid,
  FALSE AS eligibility_flag,
  'NO_EMAIL' AS ineligibility_reason
FROM (
  SELECT
    CUSTOMER_ROLES.customer_uid,
    CASE WHEN(email_address IS NULL) THEN 'FALSE'
      ELSE 'TRUE'
  END
    AS email
  FROM
    pcb-{env}-processing.domain_account_management.CUSTOMER_ROLES
  LEFT JOIN
    pcb-{env}-processing.domain_account_management.CUST_WITH_EMAIL_ACCOUNTS
  ON
    CUSTOMER_ROLES.customer_uid = CUST_WITH_EMAIL_ACCOUNTS.customer_uid)
WHERE
  email = 'FALSE';