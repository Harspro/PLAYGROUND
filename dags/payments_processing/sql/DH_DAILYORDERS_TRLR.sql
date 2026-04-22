CREATE OR REPLACE TABLE pcb-{env}-processing.domain_payments.{output_table_name}_COLUMN_TF
OPTIONS (
expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR)
)
AS
SELECT
   'TR' AS TRAILER_INDICATOR
   , COALESCE((COUNT(*) + 2),0) AS NUMBER_OF_RECORDS
   , ' ' AS FILLER
   , CAST('{file_create_dt}' AS DATE) AS FILE_CREATE_DT
   , CURRENT_DATETIME() AS REC_CREATE_TMS
FROM
    pcb-{env}-landing.domain_payments.TS2_CHQORDER_DETL
    WHERE FILE_CREATE_DT = '{file_create_dt}';