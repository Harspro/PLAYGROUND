CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) ) AS
WITH
  sa59_record AS (
  SELECT
    FILE_CREATE_DT,
    sa59.SA59_CLIENT_NUM AS CLIENT_NUM,
    sa59.SA59_ACCT_ID AS ACCT_ID,
    sa59.SA59_RECORD_ID,
    sa59.SA59_SEQ_NUM,
    sa59.SA59_KEY_STMT_FORM,
    sa59.SA59_KEY_INSRT_GRP,
    sa59.SA59_ACCOUNT_NUMB,
    sa59.SA59_NAME_PRINT,
    sa59.SA59_CUST_TYPE,
    sa59.SA59_CUST_ID,
    sa59.PARENT_CARD_NUM,
    sa59.SA59_SUB_TOTAL *100 AS SA59_SUB_TOTAL,
    sa59.SA59_PAYMENT_TRANSSUM_IND
  FROM
    `pcb-{env}-landing.domain_account_management.SA59` sa59
  WHERE
    sa59.FILE_CREATE_DT='{file_create_dt}'
    AND sa59.FILE_NAME='{file_name}'
    AND sa59.SA59_PROC_IND='Y' )
SELECT
  ((((((((((((((LPAD(CAST(sa59_record.CLIENT_NUM AS STRING), 4, '0') || LPAD(CAST(sa59_record.ACCT_ID AS STRING), 12, '0')) || sa59_record.sa59_RECORD_ID) || LPAD(CAST(sa59_record.SA59_SEQ_NUM AS STRING), 5, '0')) || ' ') || LPAD(IFNULL(sa59_record.SA59_KEY_STMT_FORM, ' '), 4, ' ')) || LPAD(IFNULL(sa59_record.SA59_KEY_INSRT_GRP, ' '), 3, ' ')) || ' ') || LPAD(IFNULL(sa59_record.SA59_ACCOUNT_NUMB, ' '), 19, ' ')) || LPAD(IFNULL(sa59_record.SA59_NAME_PRINT, ' '), 36, ' ')) || LPAD(IFNULL(sa59_record.SA59_CUST_TYPE, ' '),1,' ')) || LPAD(IFNULL(CAST(sa59_record.SA59_CUST_ID AS STRING), '0'), 9, '0')) ||
      IF
        ((sa59_record.SA59_SUB_TOTAL IS NULL ), '0000000000000+',
        IF
          ((sa59_record.SA59_SUB_TOTAL < 0), (LPAD(REPLACE(CAST((sa59_record.SA59_SUB_TOTAL * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(sa59_record.SA59_SUB_TOTAL AS STRING), '.', ''), 13, '0') || '+')))) || LPAD(IFNULL(sa59_record.SA59_PAYMENT_TRANSSUM_IND,'0'),1,'0')) || LPAD(' ', 185, ' ')) AS record,
  sa59_record.ACCT_ID,
  sa59_record.sa59_SEQ_NUM AS SEQ_NUM,
  sa59_record.FILE_CREATE_DT,
  sa59_record.SA59_RECORD_ID AS RECORD_ID,
  SUBSTR(sa59_record.PARENT_CARD_NUM,11) AS PARENT_NUM,
  8 AS RECORD_SEQ
FROM
  sa59_record