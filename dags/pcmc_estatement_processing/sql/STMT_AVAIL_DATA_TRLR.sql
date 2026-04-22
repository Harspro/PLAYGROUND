CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.STMT_AVAIL_DATA_TRLR
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) )
AS
SELECT
    "TRAILER" AS RECORD
