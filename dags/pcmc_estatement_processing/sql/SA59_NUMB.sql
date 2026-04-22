CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) ) AS
SELECT
  `pcb-{env}-landing.domain_account_management.SA59`.SA59_ACCT_ID AS ACCT_ID,
  PARENT_CARD_NUM,
  MAX( `pcb-{env}-landing.domain_account_management.SA59`.SA59_ACCOUNT_NUMB ) AS SA59_ACCOUNT_NUMB
FROM
  `pcb-{env}-landing.domain_account_management.SA59`
WHERE
  `pcb-{env}-landing.domain_account_management.SA59`.SA59_CUST_TYPE = '0'
  AND FILE_CREATE_DT = '{file_create_dt}'
  AND FILE_NAME = '{file_name}'
GROUP BY
  `pcb-{env}-landing.domain_account_management.SA59`.sa59_ACCT_ID,
  PARENT_CARD_NUM