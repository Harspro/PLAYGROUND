CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) ) AS
WITH
  SA85_record AS (
  SELECT
    FILE_CREATE_DT,
    SA85_CLIENT_NUM AS CLIENT_NUM,
    SA85_ACCT_ID AS ACCT_ID,
    SA85_RECORD_ID,
    SA85_RPAY_TOTAL_MONTHS,
    SA85_RPAY_FAIL_FLAG,
    SA85_RPAY_YEARS,
    SA85_RPAY_MONTHS_REMAINDER,
    SA85_KEY_STMT_FORM,
    SA85_KEY_INSRT_GRP,
    SA85_SEQ_NUM,
    PARENT_CARD_NUM
  FROM
    `pcb-{env}-landing.domain_account_management.SA85` SA85
  WHERE
    sa85.FILE_CREATE_DT='{file_create_dt}'
  AND sa85.FILE_NAME='{file_name}')
SELECT
  ((((((((((((LPAD(CAST(SA85_record.CLIENT_NUM AS STRING), 4, '0') || LPAD(CAST(SA85_record.ACCT_ID AS STRING), 12, '0')) || SA85_record.sa85_RECORD_ID) || LPAD(CAST(SA85_record.SA85_SEQ_NUM AS STRING), 5, '0')) || ' ') || LPAD(IFNULL(SA85_record.SA85_KEY_STMT_FORM, ' '), 4, ' ')) || LPAD(IFNULL(SA85_record.SA85_KEY_INSRT_GRP, ' '), 3, ' ')) || ' ') ||
          IF
            ((SA85_record.SA85_RPAY_TOTAL_MONTHS < 0), (LPAD(CAST((SA85_record.SA85_RPAY_TOTAL_MONTHS * -1) AS STRING), 5, '0') || '-'), ( LPAD(CAST(SA85_record.SA85_RPAY_TOTAL_MONTHS AS STRING), 5, '0') || '+'))) || LPAD(IFNULL(SA85_record.SA85_RPAY_FAIL_FLAG, ' '), 1, ' ')) || LPAD(CAST(SA85_record.SA85_RPAY_YEARS AS STRING), 4, '0')) || LPAD(CAST(SA85_record.SA85_RPAY_MONTHS_REMAINDER AS STRING), 2, '0')) || LPAD(' ', 33, ' '))AS RECORD,
  SA85_record.ACCT_ID,
  SA85_record.SA85_SEQ_NUM AS SEQ_NUM,
  SA85_record.file_create_dt,
  SA85_record.SA85_RECORD_ID AS RECORD_ID,
  SUBSTR(sa85_record.PARENT_CARD_NUM,11) AS PARENT_NUM,
  11 AS RECORD_SEQ
FROM
  SA85_record