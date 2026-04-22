CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) ) AS
WITH
  sa15_record AS (
  SELECT
    SA15.FILE_CREATE_DT AS file_create_date,
    sa15_CLIENT_NUM AS CLIENT_NUM,
    SA15_ACCT_ID AS ACCT_ID,
    sa15.* REPLACE(IF(sa15.SA15_MSG_1 != ''
      AND SA15.SA15_MSG_1 IS NOT NULL, REGEXP_REPLACE(SA15_MSG_1,'(0)0([^0]*)$', r'\1'||SUBSTR(sa10.SA10_LANG_CODE_BEG_CY,1,1)||r'\2'), SA15_MSG_1) AS SA15_MSG_1)
  FROM
    `pcb-{env}-landing.domain_account_management.SA15` sa15
  LEFT JOIN
  `pcb-{env}-landing.domain_account_management.SA10` sa10
ON
  sa10.sa10_ACCT_ID = sa15.sa15_ACCT_ID
  AND sa10.PARENT_CARD_NUM = sa15.PARENT_CARD_NUM
  AND sa15.FILE_CREATE_DT = sa10.FILE_CREATE_DT
  AND sa10.FILE_NAME = SA15.FILE_NAME
  WHERE
    sa15.file_create_dt = '{file_create_dt}'
  AND sa15.FILE_NAME='{file_name}' )
SELECT
  (((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((LPAD(CAST(sa15_record.CLIENT_NUM AS STRING), 4, '0') || LPAD(CAST(sa15_record.ACCT_ID AS STRING), 12, '0')) || sa15_record.sa15_RECORD_ID) || LPAD(CAST(sa15_record.sa15_SEQ_NUM AS STRING), 5, '0')) || ' ') || LPAD(IFNULL(sa15_record.sa15_KEY_STMT_FORM, ' '), 4, ' ')) || LPAD(IFNULL(sa15_record.sa15_KEY_INSRT_GRP, ' '), 3, ' ')) || ' ') || LPAD(IFNULL(sa15_record.SA15_SI_POCKET_1, ' '), 1, ' ')) || LPAD(IFNULL(sa15_record.SA15_SI_POCKET_2, ' '),1,' ')) || LPAD(IFNULL(sa15_record.SA15_SI_POCKET_3, ' '), 1, ' ')) || LPAD(IFNULL(sa15_record.SA15_SI_POCKET_4, ' '),1,' ')) || LPAD(IFNULL(sa15_record.SA15_SI_POCKET_5, ' '),1,' ')) || LPAD(IFNULL(sa15_record.SA15_SI_POCKET_6, ' '), 1, ' ')) || LPAD(IFNULL(sa15_record.SA15_SI_POCKET_7, ' '), 1, ' ')) || LPAD(IFNULL(sa15_record.SA15_SI_POCKET_8, ' '), 1, ' ')) || LPAD(IFNULL(sa15_record.SA15_SI_POCKET_9, ' '), 1, ' ')) || LPAD(IFNULL(sa15_record.SA15_SI_POCKET_10, ' '), 1, ' ')) || LPAD(IFNULL(sa15_record.SA15_SI_POCKET_11, ' '), 1, ' ')) || LPAD(IFNULL(sa15_record.SA15_SI_POCKET_12, ' '), 1, ' ')) || LPAD(IFNULL(sa15_record.SA15_SI_POCKET_13, ' '), 1, ' ')) || LPAD(IFNULL(sa15_record.SA15_SI_POCKET_14, ' '), 1, ' ')) || LPAD(IFNULL(sa15_record.SA15_SI_POCKET_15, ' '), 1, ' ')) || LPAD(IFNULL(sa15_record.SA15_SI_POCKET_16, ' '), 1, ' ')) || LPAD(' ', 300, ' ')) || LPAD(CAST(sa15_record.SA15_NUM_MSGS AS STRING), 3, '0')) || LPAD(IFNULL(sa15_record.SA15_MSG_1, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_2, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_3, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_4, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_5, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_6, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_7, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_8, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_9, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_10, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_11, ' '), 5, ' ')) || LPAD( IFNULL(sa15_record.SA15_MSG_12, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_13, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_14, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_15, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_16, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_17, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_18, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_19, ' '), 5, ' ')) || LPAD( IFNULL(sa15_record.SA15_MSG_20, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_21, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_22, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_23, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_24, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_25, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_26, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_27, ' '), 5, ' ')) || LPAD( IFNULL(sa15_record.SA15_MSG_28, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_29, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_30, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_31, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_32, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_33, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_34, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_35, ' '), 5, ' ')) || LPAD( IFNULL(sa15_record.SA15_MSG_36, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_37, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_38, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_39, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_40, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_41, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_42, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_43, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_44, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_45, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_46, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_47, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_48, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_49, ' '), 5, ' ')) || LPAD(IFNULL(sa15_record.SA15_MSG_50, ' '), 5, ' ')) AS record,
  sa15_record.ACCT_ID,
  sa15_record.sa15_SEQ_NUM AS SEQ_NUM,
  sa15_record.file_create_date,
  sa15_record.SA15_RECORD_ID AS RECORD_ID,
  SUBSTR(sa15_record.PARENT_CARD_NUM,11) AS PARENT_NUM,
  3 AS RECORD_SEQ
FROM
  sa15_record