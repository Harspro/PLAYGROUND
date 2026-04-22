CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) ) AS
SELECT
  SA59_ACCT_ID,
  PARENT_CARD_NUM,
  COUNT(1) AS AUTH_USER_CODE
FROM
  `pcb-{env}-landing.domain_account_management.SA59`
JOIN
  `pcb-{env}-landing.domain_account_management.STMT_VI70_CONSTANT_REF` CONST_REF
ON
  CONST_REF.INTERFACE_ID = 'VI70'
  AND CONST_REF.FIELD_NAME = 'SA88_AUTH_USER_CODE'
WHERE
  SA59_CUST_TYPE = CONST_REF.CONSTANT_VALUE
  AND FILE_CREATE_DT = '{file_create_dt}'
  AND FILE_NAME = '{file_name}'
GROUP BY
  SA59_ACCT_ID,
  PARENT_CARD_NUM