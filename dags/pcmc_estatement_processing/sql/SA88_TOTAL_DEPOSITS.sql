CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) ) AS
SELECT
  SUM(SA60_AMT_TRANS) AS TTL_DEPOSITS,
  SA60_ACCT_ID,
  PARENT_CARD_NUM
FROM
  `pcb-{env}-landing.domain_account_management.SA60`
WHERE
  FILE_CREATE_DT = '{file_create_dt}'
AND FILE_NAME='{file_name}'
AND
  SA60_DEB_CRED_IND = 'C'
GROUP BY
  SA60_ACCT_ID,
  PARENT_CARD_NUM