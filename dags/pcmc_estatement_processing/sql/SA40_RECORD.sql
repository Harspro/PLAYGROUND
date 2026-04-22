CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) ) AS
WITH
  SA40_record AS (
  SELECT
    FILE_CREATE_DT,
    SA40_ACCT_ID AS ACCT_ID,
    SA40_CLIENT_NUM AS CLIENT_num,
    SA40_RECORD_ID,
    SA40_TBAL_CODE,
    SA40_TBAL_MIN_FCHG_FLAG,
    SA40_TBAL_AMT_FCHG_BILL * 100 AS SA40_TBAL_AMT_FCHG_BILL,
    SA40_TBAL_APR_BILL * 1000000000 AS SA40_TBAL_APR_BILL,
    SA40_TBAL_MPR_DPR_BILL * 10000000 AS  SA40_TBAL_MPR_DPR_BILL,
    SA40_R_TBAL_CODE,
    SA40_TBAL_CLASS_CODE,
    SA40_TLP_TRAN_DESC,
    SA40_TBAL_DESCRIPTION,
    SA40_KEY_STMT_FORM,
    SA40_KEY_INSRT_GRP,
    SA40_SEQ_NUM,
    PARENT_CARD_NUM
  FROM
    `pcb-{env}-landing.domain_account_management.SA40` SA40
  WHERE
    sa40.FILE_CREATE_DT='{file_create_dt}'
  AND sa40.FILE_NAME='{file_name}')
SELECT
  (((((((((((((((((LPAD(CAST(SA40_record.CLIENT_NUM AS STRING), 4, '0') || LPAD(CAST(SA40_record.ACCT_ID AS STRING), 12, '0')) || SA40_record.sa40_RECORD_ID) || LPAD(CAST(SA40_record.SA40_SEQ_NUM AS STRING), 5, '0')) || ' ') || LPAD(IFNULL(SA40_record.SA40_KEY_STMT_FORM, ' '), 4, ' ')) || LPAD(IFNULL(SA40_record.SA40_KEY_INSRT_GRP, ' '), 3, ' ')) || ' ') || LPAD(CAST(SA40_record.SA40_TBAL_CODE AS STRING), 4, '0')) || IFNULL(SA40_record.SA40_TBAL_MIN_FCHG_FLAG, ' ')) ||
                IF
                  ((SA40_record.SA40_TBAL_AMT_FCHG_BILL < 0), (LPAD(REPLACE(CAST((SA40_record.SA40_TBAL_AMT_FCHG_BILL * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(SA40_record.SA40_TBAL_AMT_FCHG_BILL AS STRING ), '.', ''), 13, '0') || '+'))) ||
              IF
                ((SA40_record.SA40_TBAL_APR_BILL < 0), (LPAD(REPLACE(CAST((SA40_record.SA40_TBAL_APR_BILL * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(SA40_record.SA40_TBAL_APR_BILL AS STRING), '.', ''), 13, '0') || '+'))) ||
            IF
              ((SA40_record.SA40_TBAL_MPR_DPR_BILL < 0), (LPAD(REPLACE(CAST((SA40_record.SA40_TBAL_MPR_DPR_BILL * -1) AS STRING), '.', ''), 9, '0') || '-'), (LPAD(REPLACE(CAST(SA40_record.SA40_TBAL_MPR_DPR_BILL AS STRING ), '.', ''), 9, '0') || '+'))) || LPAD(IFNULL(SA40_record.SA40_TBAL_CLASS_CODE, ' '), 2, ' ')) || LPAD(CAST(SA40_record.SA40_R_TBAL_CODE AS STRING), 4, '0')) || LPAD(IFNULL(SA40_record.SA40_TLP_TRAN_DESC, ' '), 41, ' ')) || LPAD(IFNULL(SA40_record.SA40_TBAL_DESCRIPTION, ' '), 40, ' ')) || LPAD(' ', 36, ' ')) AS record,
  SA40_record.ACCT_ID,
  SA40_record.SA40_SEQ_NUM AS SEQ_NUM,
  SA40_record.file_create_dt,
  SA40_record.SA40_RECORD_id AS RECORD_ID,
  SUBSTR(sa40_record.PARENT_CARD_NUM,11) AS PARENT_NUM,
  6 AS RECORD_SEQ
FROM
  SA40_record