CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) ) AS
WITH
  sa99_record AS (
  SELECT
    SA99_CLIENT_NUM AS client_num,
    SA99_ACCT_ID AS ACCT_ID,
    SA99_RECORD_ID,
    SA99_PROCESS_DATE,
    SA99_RUN_DATE,
    SA99_RUN_TIME,
    SA99_RUN_DAY_OF_WEEK,
    SA99_TOTAL_ACCTS,
    SA99_CLIENT_NAME,
    SA99_REC_COUNT,
    SA99_SEQ_NUM,
    SA99_KEY_STMT_FORM,
    SA99_KEY_INSRT_GRP,
    SA99_OUT_OF_BAL_COUNT,
    SA99_OUT_OF_BAL_COUNT,
    FILE_CREATE_DT
  FROM
    `pcb-{env}-landing.domain_account_management.SA99` sa99
  WHERE
    sa99.file_create_dt='{file_create_dt}'
    AND FILE_NAME = '{file_name}')
,
  rec_cycle_count AS (
  SELECT
    DISTINCT SA10_BILLING_CYCLE AS CYCLE_NO
  FROM
    pcb-{env}-landing.domain_account_management.SA10
  WHERE
    file_create_dt = '{file_create_dt}'
  AND FILE_NAME = '{file_name}')
,
  detl_count AS (
    SELECT count(*) AS DETL_COUNT , CYCLE_NO
    FROM
      pcb-{env}-processing.domain_account_management.PCB_DST_STMT_DETL_DBEXT
    WHERE
      FILE_CREATE_DT = '{file_create_dt}'
    GROUP BY CYCLE_NO
  )
,
  sa10_rec_count AS (
    SELECT COUNT(*) AS SA10_REC_COUNT, CYCLE_NO
    FROM
      pcb-{env}-processing.domain_account_management.PCB_DST_STMT_DETL_DBEXT
    WHERE
     FILE_CREATE_DT = '{file_create_dt}'
     AND
     RECORD_ID = 'SA10'
    GROUP BY CYCLE_NO
  )

SELECT
  ((((((((((((((((((((((((LPAD(CAST(sa99_record.CLIENT_NUM AS STRING), 4, '0') || LPAD(CAST(sa99_record.ACCT_ID AS STRING), 12, '0')) || sa99_record.sa99_RECORD_ID) || '00000') || ' ') || LPAD(IFNULL(sa99_record.sa99_KEY_STMT_FORM, ' '), 4, ' ')) || LPAD(IFNULL(sa99_record.SA99_KEY_INSRT_GRP, ' '), 3, ' ')) || ' ') || LPAD(sa99_record.SA99_PROCESS_DATE, 10, ' ')) || ' ') || sa99_record.SA99_RUN_DATE) || ' ') || sa99_record.SA99_RUN_TIME) || ' ') || LPAD(sa99_record.SA99_RUN_DAY_OF_WEEK, 9, ' ')) || ' ') || LPAD(sa99_record.sa99_CLIENT_NAME, 30, ' ')) || ' ') || LPAD(CAST(detl_count.DETL_COUNT AS STRING), 11, '0')) || ' ') || LPAD(CAST(sa10_rec_count.SA10_REC_COUNT AS STRING), 11, '0')) || ' ') || '00000000000') || LPAD(' ', 1, ' ')) || LPAD(' ', 369, ' ')) AS record,
  SA99_record.ACCT_ID,
  SA99_record.SA99_SEQ_NUM AS SEQ_NUM,
  SA99_record.file_create_dt,
  SA99_record.SA99_RECORD_ID AS RECORD_ID,
  '0' AS PARENT_NUM,
  0 AS RECORD_SEQ,
  rec_cycle_count.CYCLE_NO
FROM
  detl_count
  LEFT JOIN sa10_rec_count ON detl_count.CYCLE_NO = sa10_rec_count.CYCLE_NO
  RIGHT JOIN rec_cycle_count ON sa10_rec_count.CYCLE_NO = rec_cycle_count.CYCLE_NO
  CROSS JOIN sa99_record;

