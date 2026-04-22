CREATE OR REPLACE VIEW
  pcb-{env}-processing.domain_account_management.CUSTOMER_BLOCK_CODES
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS
SELECT
  cifp_account_id5,
  cifp_block_code,
  customer_uid
FROM
  pcb-{env}-processing.domain_account_management.CLOSED_BLOCK_CODES
UNION ALL
SELECT
  cifp_account_id5,
  cifp_block_code,
  customer_uid
FROM
  pcb-{env}-processing.domain_account_management.OTHER_BLOCK_CODES;