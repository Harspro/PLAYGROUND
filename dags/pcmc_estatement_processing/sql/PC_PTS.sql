CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) ) AS
SELECT
  SUM(PC_POINTS) AS PC_POINTS,
  0 AS PC_BONUS_POINTS,
  SA60_ACCT_ID,
  PARENT_CARD_NUM
FROM (
  SELECT
    SUM( ROUND( SA60_AMT_TRANS * SAFE_CAST( (
          SELECT
            constant_value
          FROM
            `pcb-{env}-landing.domain_account_management.STMT_VI70_CONSTANT_REF`
          WHERE
            interface_id = 'VI70'
            AND field_name = 'SA88_PC_POINTS_1' ) AS NUMERIC ) ) ) AS PC_POINTS,
    SA60_ACCT_ID,
    PARENT_CARD_NUM
  FROM
    `pcb-{env}-landing.domain_account_management.SA60`
  JOIN
    `pcb-{env}-landing.domain_account_management.STMT_VI70_TCATTRANS_LOOKUP` lkp
  ON
    SA60_TRANS_CODE = lkp.TRANS_CODE
  WHERE
    file_create_dt = '{file_create_dt}'
    AND FILE_NAME = '{file_name}'
    AND
    SA60_DEB_CRED_IND = 'D'
    AND IFNULL(SA60_TRANS_SUM_INDICATOR, 'X') != 'Y'
    AND lkp.TCAT IN (
    SELECT
      SAFE_CAST(CONSTANT_VALUE AS INT64)
    FROM
      `pcb-{env}-landing.domain_account_management.STMT_VI70_CONSTANT_REF`
    WHERE
      interface_id = 'VI70'
      AND FIELD_NAME IN ( 'SA88_PC_POINTS_2',
        'SA88_PC_POINTS_3',
        'SA88_PC_POINTS_4',
        'SA88_PC_POINTS_5',
        'SA88_PC_POINTS_6' ) )
    AND ( SA60_TRANS_SUM_INDICATOR IS NULL
      OR SA60_TRANS_SUM_INDICATOR = 'N' )
  GROUP BY
    SA60_ACCT_ID,
    PARENT_CARD_NUM
  UNION ALL
  SELECT
    SUM( ROUND( SA60_AMT_TRANS * - SAFE_CAST( (
          SELECT
            constant_value
          FROM
            `pcb-{env}-landing.domain_account_management.STMT_VI70_CONSTANT_REF`
          WHERE
            interface_id = 'VI70'
            AND field_name = 'SA88_PC_POINTS_1' ) AS NUMERIC ) ) ) AS PC_POINTS,
    SA60_ACCT_ID,
    PARENT_CARD_NUM
  FROM
    `pcb-{env}-landing.domain_account_management.SA60`
  JOIN
    `pcb-{env}-landing.domain_account_management.STMT_VI70_TCATTRANS_LOOKUP` lkp
  ON
    SA60_TRANS_CODE = lkp.TRANS_CODE
  WHERE
    file_create_dt = '{file_create_dt}'
    AND FILE_NAME = '{file_name}'
    AND
    SA60_DEB_CRED_IND = 'C'
    AND lkp.TCAT IN (
    SELECT
      SAFE_CAST(CONSTANT_VALUE AS INT64)
    FROM
      `pcb-{env}-landing.domain_account_management.STMT_VI70_CONSTANT_REF`
    WHERE
      interface_id = 'VI70'
      AND FIELD_NAME IN ( 'SA88_PC_POINTS_2',
        'SA88_PC_POINTS_3',
        'SA88_PC_POINTS_4',
        'SA88_PC_POINTS_5',
        'SA88_PC_POINTS_6' ) )
    AND ( SA60_TRANS_SUM_INDICATOR IS NULL
      OR SA60_TRANS_SUM_INDICATOR = 'N' )
  GROUP BY
    SA60_ACCT_ID,
    PARENT_CARD_NUM )
GROUP BY
  SA60_ACCT_ID,
  PARENT_CARD_NUM