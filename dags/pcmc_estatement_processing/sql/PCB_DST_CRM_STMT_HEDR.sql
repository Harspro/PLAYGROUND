CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) ) AS
WITH SA00_record AS (
    SELECT
        SA00_CLIENT_NUM AS CLIENT_NUM,
        SA00_FILE_SEQ_NBR AS FILE_SEQ,
        SA00_PROCESS_DATE
    FROM `pcb-{env}-landing.domain_account_management.SA00`
    WHERE FILE_CREATE_DT = '{file_create_dt}'
    AND FILE_NAME = '{file_name}'
),

REC_CYCLE_COUNT as (
SELECT distinct SA10_BILLING_CYCLE AS CYCLE_NO
FROM pcb-{env}-landing.domain_account_management.SA10
WHERE FILE_CREATE_DT = '{file_create_dt}'
AND FILE_NAME = '{file_name}'
)

SELECT
    CONCAT(
        '1H',
        LPAD('CRM Target Data', 17, ' '),
        LPAD(CAST(SA00_record.CLIENT_NUM AS STRING), 4, ' '),
        LPAD(CAST(SA00_record.FILE_SEQ AS STRING), 6, '0'),
        FORMAT_DATE('%Y-%m-%d', CURRENT_DATE('America/Toronto'))
    ) AS record,
     9999 AS TSYS_ACCOUNT_ID,
     'H' AS REC_TYPE,
     CYCLE_NO
FROM SA00_record
CROSS JOIN REC_CYCLE_COUNT ;