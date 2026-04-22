CREATE OR REPLACE VIEW
  pcb-{env}-processing.domain_account_management.PERMITTED_CUSTOMERS
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS
SELECT
  DISTINCT CUST_BASE.customer_uid
FROM
  pcb-{env}-processing.domain_account_management.ENTITLED_CUSTOMERS CUST_BASE
INNER JOIN
  pcb-{env}-processing.domain_account_management.CUST_WITH_EMAIL_ACCOUNTS CUST_WITH_EMAIL
ON
  CUST_BASE.customer_uid = CUST_WITH_EMAIL.customer_uid
LEFT JOIN
  pcb-{env}-processing.domain_account_management.PCMA_ABB_ACCOUNTS PCMA_ABB
ON
  CUST_BASE.customer_uid = PCMA_ABB.customer_uid
LEFT JOIN
  pcb-{env}-processing.domain_account_management.OTHER_BLOCK_CODES BLOCK_CODES
ON
  CUST_BASE.customer_uid = BLOCK_CODES.customer_uid
LEFT JOIN
  pcb-{env}-processing.domain_account_management.CUSTOMER_BAD_STANDING_ACCOUNTS BAD_STANDING_ACC
ON
  CUST_BASE.customer_uid = BAD_STANDING_ACC.customer_uid
WHERE
  BLOCK_CODES.cifp_account_id5 IS NULL
  AND PCMA_ABB.customer_uid IS NULL
  AND BAD_STANDING_ACC.customer_uid IS NULL
  AND CUST_BASE.customer_uid NOT IN (9647425,
    9658200,
    8365290);