CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) )
AS
SELECT
  *
FROM
  pcb-{env}-landing.domain_account_management.TSYS_RMS_HEDR
WHERE
  FILE_CREATE_DT = '{file_create_dt}';