CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) )
AS
WITH
  -- Subquery to calculate REC1-Values
  REC1_STATS AS (
    SELECT
      COUNT(*) AS REC1_TOTAL,
      IFNULL(SUM(ACCOUNT_BALANCE), 0) AS REC1_CURR_BALANCE
    FROM
      pcb-{env}-processing.domain_account_management.RMS_SCORE_DETL01
    WHERE
      FILE_CREATE_DT = '{file_create_dt}'
  ),

  -- Subquery to calculate REC3-Values
  REC3_STATS AS (
    SELECT
      COUNT(*) AS REC3_TOTAL,
      IFNULL(SUM(CASE WHEN DEBIT_CREDIT_IND = 'C' THEN TRANSACTION_AMOUNT ELSE 0 END), 0) AS REC3_TOT_CREDIT_AMT,
      COUNT(CASE WHEN DEBIT_CREDIT_IND = 'C' THEN TRANSACTION_AMOUNT ELSE NULL END) AS REC3_TOT_NUM_CREDIT,
      IFNULL(SUM(CASE WHEN DEBIT_CREDIT_IND = 'D' THEN TRANSACTION_AMOUNT ELSE 0 END), 0) AS REC3_TOT_DEBIT_AMT,
      COUNT(CASE WHEN DEBIT_CREDIT_IND = 'D' THEN TRANSACTION_AMOUNT ELSE NULL END) AS REC3_TOT_NUM_DEBIT
    FROM
      pcb-{env}-processing.domain_account_management.RMS_SCORE_DETL03
    WHERE
      FILE_CREATE_DT = '{file_create_dt}'
  ),

  -- Subquery to calculate REC4-Values
  REC4_STATS AS (
    SELECT
      COUNT(*) AS REC4_TOTAL
    FROM
      pcb-{env}-processing.domain_account_management.RMS_SCORE_DETL04
    WHERE
      FILE_CREATE_DT = '{file_create_dt}'
  )

SELECT
  TRAILER_ID,
  REC1,
  r1.REC1_TOTAL,
  SIGN_REC1_CURR_BALANCE,
  r1.REC1_CURR_BALANCE,
  REC2,
  CAST('0' AS INTEGER) AS REC2_TOTAL,
  REC3,
  r3.REC3_TOTAL,
  r3.REC3_TOT_NUM_CREDIT,
  SIGN_REC3_TOT_CREDIT_AMT,
  r3.REC3_TOT_CREDIT_AMT,
  r3.REC3_TOT_NUM_DEBIT,
  SIGN_REC3_TOT_DEBIT_AMT,
  r3.REC3_TOT_DEBIT_AMT,
  REC4,
  r4.REC4_TOTAL,
  REC5,
  CAST('0' AS INTEGER) AS REC5_TOTAL,
  TRAILER_FILLER,
  FILE_CREATE_DT,
  REC_LOAD_TIMESTAMP
FROM
  pcb-{env}-landing.domain_account_management.TSYS_RMS_TRLR tr
LEFT JOIN REC1_STATS r1 ON tr.FILE_CREATE_DT = '{file_create_dt}'
LEFT JOIN REC3_STATS r3 ON tr.FILE_CREATE_DT = '{file_create_dt}'
LEFT JOIN REC4_STATS r4 ON tr.FILE_CREATE_DT = '{file_create_dt}'
WHERE
  tr.FILE_CREATE_DT = '{file_create_dt}';