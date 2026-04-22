CREATE OR REPLACE VIEW
  pcb-{env}-processing.domain_account_management.PCMA_ACTIVE_ACCOUNTS
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS
SELECT
  CAST(ACCOUNT.mast_account_id AS int64) AS account_number,
  ACCOUNT_CUSTOMER.account_uid,
  ACCOUNT_CUSTOMER.customer_uid
FROM
  pcb-{env}-curated.domain_account_management.ACCOUNT
INNER JOIN
  pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER
ON
  ACCOUNT.account_uid = ACCOUNT_CUSTOMER.account_uid
WHERE
  ACCOUNT_CUSTOMER.account_customer_role_uid = 1
  AND ACCOUNT.product_uid = 7
  AND ACCOUNT_CUSTOMER.active_ind = 'Y'
  AND ACCOUNT_CUSTOMER.CUSTOMER_UID NOT IN (9647425,
    9658200,
    8365290);