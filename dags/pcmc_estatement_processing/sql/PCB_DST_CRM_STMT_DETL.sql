CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) ) AS

SELECT
  *
FROM
  pcb-{env}-processing.domain_account_management.PCB_DST_CRM_STMT_HEDR_DBEXT
UNION ALL
SELECT
  *
FROM
  pcb-{env}-processing.domain_account_management.OFFER_RECORDS_DBEXT
UNION ALL
SELECT
  *
FROM
  pcb-{env}-processing.domain_account_management.CUSTOMER_RECORDS_DBEXT