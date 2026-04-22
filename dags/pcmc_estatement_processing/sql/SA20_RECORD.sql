CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) ) AS
WITH
  sa20_record AS (
  SELECT
    SA20_ACCT_ID AS ACCT_ID,
    SA20_CLIENT_NUM AS CLIENT_num,
    SA20_RECORD_ID,
    SA20_NAME_1,
    SA20_ADDRESS_LINE1,
    SA20_ADDRESS_LINE2,
    SA20_CITY,
    SA20_STATE_PROV_CODE,
    SA20_COUNTRY_CODE,
    SA20_COUNTRY_NAME,
    SA20_JUNK_MAIL_FLAG,
    SA20_DATE_BIRTH,
    SA20_POS_FIRST_NAME,
    SA20_POS_MIDDLE_NAME,
    SA20_POS_LAST_NAME,
    SA20_PRIM_CMID,
    SA20_SEQ_NUM,
    SA20_KEY_STMT_FORM,
    SA20_KEY_INSRT_GRP,
    SA20_ZIP_POSTAL_CODE,
    FILE_CREATE_DT,
    PARENT_CARD_NUM
  FROM
    `pcb-{env}-landing.domain_account_management.SA20` sa20
  WHERE
    sa20.FILE_CREATE_DT='{file_create_dt}'
  AND sa20.FILE_NAME='{file_name}')
SELECT
  ((((((((((((((((((((((LPAD(SAFE_CAST(sa20_record.CLIENT_NUM AS STRING), 4, '0') || LPAD(SAFE_CAST(sa20_record.ACCT_ID AS STRING), 12, '0')) || sa20_record.sa20_RECORD_ID) || LPAD(SAFE_CAST(sa20_record.sa20_SEQ_NUM AS STRING), 5, '0')) || ' ') || LPAD(IFNULL(sa20_record.SA20_KEY_STMT_FORM, ' '), 4, ' ')) || LPAD(IFNULL(sa20_record.SA20_KEY_INSRT_GRP, ' '), 3, ' ')) || ' ') || LPAD(IFNULL(sa20_record.SA20_NAME_1, ' '), 36, ' ')) || LPAD(IFNULL(sa20_record.SA20_ADDRESS_LINE1, ' '), 40, ' ')) || LPAD(IFNULL(sa20_record.SA20_ADDRESS_LINE2, ' '), 40, ' ')) || LPAD(IFNULL(sa20_record.SA20_CITY, ' '), 19, ' ')) || LPAD(IFNULL(sa20_record.SA20_STATE_PROV_CODE, ' '), 3, ' ')) || LPAD(IFNULL(sa20_record.SA20_ZIP_POSTAL_CODE, ' '), 13, ' ')) || LPAD(IFNULL(sa20_record.SA20_COUNTRY_CODE, ' '), 3, ' ')) || LPAD(IFNULL(sa20_record.SA20_DATE_BIRTH, ' '), 8, ' ')) || LPAD(SAFE_CAST(sa20_record.SA20_POS_FIRST_NAME AS STRING), 2, '0')) || LPAD(SAFE_CAST(sa20_record.SA20_POS_MIDDLE_NAME AS STRING), 2, '0')) || LPAD(SAFE_CAST(sa20_record.SA20_POS_LAST_NAME AS STRING), 2, '0')) || LPAD(IFNULL(sa20_record.SA20_PRIM_CMID, ' '), 8, ' ')) || LPAD(IFNULL(sa20_record.SA20_JUNK_MAIL_FLAG,' '),1 ,' ')) || LPAD(IFNULL(sa20_record.SA20_COUNTRY_NAME, ' '), 25, ' ')) || LPAD(' ', 64, ' ')) AS record,
  sa20_record.ACCT_ID,
  sa20_record.sa20_SEQ_NUM AS SEQ_NUM,
  sa20_record.file_create_dt,
  SA20_RECORD.SA20_RECORD_ID AS RECORD_ID,
  SUBSTR(sa20_record.PARENT_CARD_NUM,11) AS PARENT_NUM,
  3 AS RECORD_SEQ
FROM
  sa20_record