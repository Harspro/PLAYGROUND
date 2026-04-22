CREATE OR REPLACE VIEW
  pcb-{env}-processing.domain_account_management.ENTITLED_CUSTOMERS
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS
SELECT
  ACTIVE_CUSTOMER_BASE.customer_uid
FROM
  pcb-{env}-processing.domain_account_management.ACTIVE_CUSTOMER_BASE
LEFT JOIN
  pcb-{env}-processing.domain_account_management.CLOSED_BLOCK_CODES
ON
  ACTIVE_CUSTOMER_BASE.account_no = CLOSED_BLOCK_CODES.cifp_account_id5
WHERE
  CLOSED_BLOCK_CODES.cifp_account_id5 IS NULL;