CREATE OR REPLACE VIEW
  pcb-{env}-processing.domain_account_management.CUST_WITH_EMAIL_ACCOUNTS
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS
SELECT
  a.customer_uid,
  b.email_address
FROM
  `pcb-{env}-curated.domain_customer_management.CONTACT` a
JOIN
  pcb-{env}-curated.domain_customer_management.EMAIL_CONTACT b
ON
  a.contact_uid=b.contact_uid
WHERE
  a.context='PRIMARY'
  AND a.type='EMAIL';