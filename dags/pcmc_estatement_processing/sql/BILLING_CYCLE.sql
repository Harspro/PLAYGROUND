CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) ) AS
SELECT
  DISTINCT SA10_BILLING_CYCLE,
  SA10_ACCT_ID,
  PARENT_CARD_NUM,
  SUBSTR(PARENT_CARD_NUM,11) AS PARENT_NUM,
  FILE_CREATE_DT,
  SA10_CLIENT_PROD_CODE,
  FILE_NAME
FROM
      `pcb-{env}-landing.domain_account_management.SA10`
WHERE
  file_create_dt = '{file_create_dt}'
AND
  file_name = '{file_name}'