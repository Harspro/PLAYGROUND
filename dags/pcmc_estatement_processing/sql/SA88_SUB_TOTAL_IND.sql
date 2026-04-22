CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) ) AS
SELECT
  sa59_acct_id,
  parent_card_num,
  COUNT (DISTINCT(SA59_ACCOUNT_NUMB)) AS COUNT_ACCT_NUM
FROM
  `pcb-{env}-landing.domain_account_management.SA59`
WHERE
  FILE_CREATE_DT = '{file_create_dt}'
AND FILE_NAME='{file_name}'
AND
  IFNULL(SA59_PAYMENT_TRANSSUM_IND, '0') <> '1'
GROUP BY
  sa59_acct_id,
  parent_card_num