CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_customer_management.RETURNCFF_CME_CUSTOMER_TEMP
  OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR))
AS
SELECT
  *,
  DATETIME(CURRENT_TIMESTAMP(), "America/Toronto") AS REC_CHNG_TMS
FROM
  pcb-{env}-processing.domain_customer_management.STMT_UPDATE_CUST