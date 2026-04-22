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
    AND FIELD_NAME IN ( 'SA88_FEES_1',
      'SA88_FEES_2',
      'SA88_FEES_3',
      'SA88_FEES_4',
      'SA88_FEES_5',
      'SA88_FEES_6',
      'SA88_FEES_7',
      'SA88_FEES_8',
      'SA88_FEES_9',
      'SA88_FEES_10',
      'SA88_FEES_11',
      'SA88_FEES_12',
      'SA88_FEES_13',
      'SA88_FEES_14',
      'SA88_FEES_15',
      'SA88_FEES_16',
      'SA88_FEES_17' ) )
GROUP BY
  ACCOUNT_ID,
  PARENT_CARD_NUM