CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name} OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) ) AS
WITH
  COMBINED AS (
  SELECT
    *
  FROM
    pcb-{env}-processing.domain_account_management.PCB_DST_CRM_STMT_DETL_DBEXT
  UNION ALL
  SELECT
    *
  FROM
    pcb-{env}-processing.domain_account_management.PCB_DST_CRM_STMT_TRLR_DBEXT
  ORDER BY
    record ),
  NUMBERED AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY CYCLE_NO ORDER BY CASE REC_TYPE WHEN 'H' THEN 1 WHEN 'O' THEN 2 WHEN 'C' THEN 3 WHEN 'T' THEN 4 END) AS RN
  FROM
    COMBINED )
SELECT
  REC_TYPE || LPAD(CAST(RN AS STRING), 6, '0') || SUBSTR(record, 3) AS record,
  CYCLE_NO,
  RN AS SEQ_NUM
FROM
  NUMBERED