CREATE OR REPLACE VIEW
  pcb-{env}-processing.domain_account_management.PCMA_ABB_ACCOUNTS
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS
SELECT
  DISTINCT PCMA_ACTIVE_ACCOUNTS.customer_uid
FROM
  pcb-{env}-processing.domain_account_management.ABB_ACCOUNTS
INNER JOIN
  pcb-{env}-processing.domain_account_management.PCMA_ACTIVE_ACCOUNTS
ON
  ABB_ACCOUNTS.account_number = PCMA_ACTIVE_ACCOUNTS.account_number
LEFT JOIN
  `pcb-{env}-curated.domain_validation_verification.CUSTOMER_ID_VERIFICATION` CUST_IDV_INFO
ON
  PCMA_ACTIVE_ACCOUNTS.CUSTOMER_UID = CUST_IDV_INFO.CUSTOMER_UID
WHERE
  CUST_IDV_INFO.CUSTOMER_UID IS NULL;