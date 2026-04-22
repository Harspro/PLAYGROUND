CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) ) AS
SELECT
  SUM(AMT_CTD) AS AMT_CTD,
  ACCOUNT_ID,
  PARENT_CARD_NUM
FROM (
  SELECT
    SUM(AMT_CTD) AS AMT_CTD,
    ACCOUNT_ID,
    PARENT_CARD_NUM
  FROM
    `pcb-{env}-landing.domain_account_management.SA30_TSYS_BUCKETS`
  WHERE
    FILE_CREATE_DT = '{file_create_dt}'
  AND
    SAFE_CAST(TSYS_BUCKET_NO AS STRING) IN (
    SELECT
      CONSTANT_VALUE
    FROM
      `pcb-{env}-landing.domain_account_management.STMT_VI70_CONSTANT_REF`
    WHERE
      interface_id = 'VI70'
      AND FIELD_NAME IN ( 'SA88_PURCHASES_1',
        'SA88_PURCHASES_2',
        'SA88_PURCHASES_3' ) )
  GROUP BY
    ACCOUNT_ID,
    PARENT_CARD_NUM
  UNION ALL
  SELECT
    SUM(AMT_CTD) * -1 AS AMT_CTD,
    ACCOUNT_ID,
    PARENT_CARD_NUM
  FROM
    `pcb-{env}-landing.domain_account_management.SA30_TSYS_BUCKETS`
  WHERE
    FILE_CREATE_DT = '{file_create_dt}'
  AND FILE_NAME='{file_name}'
  AND
    SAFE_CAST(TCAT AS STRING) = IFNULL( (
      SELECT
        CONSTANT_VALUE
      FROM
        `pcb-{env}-landing.domain_account_management.STMT_VI70_CONSTANT_REF`
      WHERE
        interface_id = 'VI70'
        AND FIELD_NAME = 'SA88_PURCHASES_4' ), '31' )
  GROUP BY
    ACCOUNT_ID,
    PARENT_CARD_NUM )
GROUP BY
  ACCOUNT_ID,
  PARENT_CARD_NUM