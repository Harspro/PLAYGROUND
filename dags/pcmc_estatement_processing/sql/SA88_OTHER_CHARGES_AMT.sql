CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) ) AS
SELECT
  SUM(AMT_CTD) AS amt_ctd,
  ACCOUNT_ID,
  PARENT_CARD_NUM
FROM
  `pcb-{env}-landing.domain_account_management.SA30_TSYS_BUCKETS`
WHERE
  FILE_CREATE_DT = '{file_create_dt}'
AND FILE_NAME='{file_name}'
AND
  SAFE_CAST(TSYS_BUCKET_NO AS STRING) IN (
  SELECT
    CONSTANT_VALUE
  FROM
    `pcb-{env}-landing.domain_account_management.STMT_VI70_CONSTANT_REF`
  WHERE
    interface_id = 'VI70'
    AND FIELD_NAME IN ( 'SA88_OTHER_CHARGES_1',
      'SA88_OTHER_CHARGES_2',
      'SA88_OTHER_CHARGES_3',
      'SA88_OTHER_CHARGES_4',
      'SA88_OTHER_CHARGES_5' ) )
GROUP BY
  ACCOUNT_ID,
  PARENT_CARD_NUM