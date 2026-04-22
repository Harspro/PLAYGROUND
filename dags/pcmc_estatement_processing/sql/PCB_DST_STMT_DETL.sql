CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) )
  AS
  WITH
  COMBINED AS(
  SELECT
    DL.*,
    SA10.SA10_BILLING_CYCLE AS CYCLE_NO
  FROM
    pcb-{env}-processing.domain_account_management.PCB_DST_STMT_DETL_INT_DBEXT AS DL
  LEFT JOIN
    `pcb-{env}-processing.domain_account_management.BILLING_CYCLE_DBEXT` AS SA10
  ON
    DL.ACCT_ID = SA10.SA10_ACCT_ID
    AND DL.FILE_CREATE_DT = SA10.FILE_CREATE_DT
    AND DL.PARENT_NUM = SA10.PARENT_NUM
    AND SA10.FILE_CREATE_DT = '{file_create_dt}'
    AND SA10.SA10_CLIENT_PROD_CODE != 'PDG'
  ORDER BY
    ACCT_ID,
    SEQ_NUM),
  NUMBERED AS(
  SELECT
    *,
    ROW_NUMBER()
      OVER (
        PARTITION BY
          ACCT_ID,
          PARENT_NUM,
          CYCLE_NO
        ORDER BY
          CASE RECORD_ID
            WHEN 'SA10'
              THEN 1
            WHEN 'SA14'
              THEN 2
            WHEN 'SA15'
              THEN 3
            WHEN 'SA20'
              THEN 4
            WHEN 'SA30'
              THEN 5
            WHEN 'SA40'
              THEN 6
            WHEN 'SA59'
              THEN 7
            WHEN 'SA60'
              THEN 8
            WHEN 'SA85'
              THEN 10
            WHEN 'SA88'
              THEN 11
          END,
          SEQ_NUM
      ) AS RN
  FROM
    COMBINED )
SELECT
  * REPLACE(CONCAT(SUBSTR(RECORD,1,20),LPAD(CAST(RN AS STRING),5,'0'),SUBSTR(RECORD,26)) AS RECORD, RN AS SEQ_NUM)
FROM
  NUMBERED
ORDER BY
  ACCT_ID,
  PARENT_NUM,
  SEQ_NUM