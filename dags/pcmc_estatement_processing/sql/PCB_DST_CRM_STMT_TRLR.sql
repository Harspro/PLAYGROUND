CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) ) AS
WITH
  OFFER_COUNT AS (
  SELECT
    CYCLE_NO, COUNT(*) AS OFFER_COUNT
  FROM
    pcb-{env}-processing.domain_account_management.OFFER_RECORDS_DBEXT
  GROUP BY CYCLE_NO),
  CUSTOMER_COUNT AS (
  SELECT
    CYCLE_NO, COUNT(*) AS CUST_COUNT
  FROM
    pcb-{env}-processing.domain_account_management.CUSTOMER_RECORDS_DBEXT
  GROUP BY CYCLE_NO),
  COMBINED_COUNTS AS (
  SELECT
    O.OFFER_COUNT,
    C.CUST_COUNT,
    (O.OFFER_COUNT + C.CUST_COUNT) AS TOTAL_COUNT,
    O.CYCLE_NO AS CYCLE_NO
  FROM
    OFFER_COUNT O
  JOIN
    CUSTOMER_COUNT C
  ON O.CYCLE_NO = C.CYCLE_NO),
  REC_CYCLE_COUNT AS (
  SELECT
    DISTINCT SA10_BILLING_CYCLE AS CYCLE_NO
  FROM
    `pcb-{env}-landing.domain_account_management.SA10`
  WHERE
    FILE_CREATE_DT = '{file_create_dt}'
    AND FILE_NAME = '{file_name}')
SELECT
  '4T' || LPAD('CRM Target Data', 17, ' ') || LPAD(CAST(SA99.SA99_CLIENT_NUM AS STRING), 4, ' ') || LPAD(CAST(IFNULL(CC.offer_count, 0) AS STRING), 4, '0') || LPAD(CAST(IFNULL(CC.cust_count, 0) AS STRING), 13, '0') || LPAD(CAST(IFNULL(CC.total_count, 0) AS STRING), 13, '0') || LPAD(CAST(SA99.SA99_SEQ_NUM AS STRING), 6, '0') || FORMAT_DATE('%Y-%m-%d', CURRENT_DATE('America/Toronto')) || LPAD(' ', 26, ' ') AS record_seq,
  9999 AS TSYS_ACCOUNT_ID,
  'T' AS REC_TYPE,
  RC.CYCLE_NO AS CYCLE_NO
FROM
  REC_CYCLE_COUNT RC
LEFT JOIN
  COMBINED_COUNTS CC
  ON RC.CYCLE_NO = CC.CYCLE_NO
CROSS JOIN
  pcb-{env}-landing.domain_account_management.SA99 AS SA99
WHERE
  FILE_CREATE_DT = '{file_create_dt}'
  AND FILE_NAME = '{file_name}'