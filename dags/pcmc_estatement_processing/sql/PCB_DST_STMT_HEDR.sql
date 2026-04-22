CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) ) AS
WITH
  SA00_record AS (
  SELECT
    SA00_CLIENT_NUM AS CLIENT_NUM,
    SA00_ACCT_ID AS ACCT_ID,
    SA00_SEQ_NUM,
    SA00.SA00_RECORD_ID,
    sa00.SA00_PROCESS_DATE,
    sa00.SA00_RUN_DATE,
    sa00.SA00_RUN_TIME,
    sa00.SA00_RUN_DAY_OF_WEEK,
    sa00.SA00_CLIENT_NAME,
    sa00.SA00_FILE_SEQ_NBR,
    sa00.SA00_KEY_STMT_FORM,
    sa00.SA00_KEY_INSRT_GRP,
    FILE_CREATE_DT AS file_create_date
  FROM
    `pcb-{env}-landing.domain_account_management.SA00` sa00
  WHERE
    file_create_dt = '{file_create_dt}'
  AND FILE_NAME = '{file_name}'),
  rec_cycle_count AS (
  SELECT
    DISTINCT SA10_BILLING_CYCLE AS CYCLE_NO
  FROM
    pcb-{env}-landing.domain_account_management.SA10
  WHERE
    file_create_dt = '{file_create_dt}'
  AND FILE_NAME = '{file_name}')
SELECT
  LPAD(CAST(SA00_record.CLIENT_NUM AS STRING), 4, '0') || LPAD(CAST(SA00_record.ACCT_ID AS STRING), 12, '0') || SA00_record.sa00_RECORD_ID || '00000' || ' ' || LPAD(IFNULL(SA00_record.SA00_KEY_STMT_FORM, ' '), 4, ' ') || LPAD(IFNULL(SA00_record.SA00_KEY_INSRT_GRP, ' '), 3, ' ') || ' ' || LPAD(SA00_record.SA00_PROCESS_DATE, 10, ' ') || ' ' || SA00_RUN_DATE || ' ' || SA00_RUN_TIME || ' ' || LPAD(SA00_RUN_DAY_OF_WEEK, 9, ' ') || ' ' || LPAD(SA00_record.SA00_CLIENT_NAME, 30, ' ') || ' ' || LPAD(CAST(SA00_FILE_SEQ_NBR AS STRING), 5, '0') || ' ' || LPAD(' ', 399, ' ') AS record,
  SA00_record.ACCT_ID,
  SA00_record.SA00_SEQ_NUM AS SEQ_NUM,
  SA00_record.file_create_date,
  SA00_record.SA00_RECORD_ID AS RECORD_ID,
  '0' AS PARENT_NUM,
  0 AS RECORD_SEQ,
  CYCLE_NO
FROM
  SA00_record
CROSS JOIN
  rec_cycle_count ;