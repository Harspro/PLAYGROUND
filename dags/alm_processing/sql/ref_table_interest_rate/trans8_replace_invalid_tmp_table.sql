CREATE OR REPLACE TABLE `{bq_invalid_tmp_table_id}`
OPTIONS (
    expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR),
    description = "Temporary table for storing extracted data"
    ) AS 
    SELECT *
    FROM `{bq_invalid_table_id}`
    WHERE LOAD_DATE = ( SELECT LOAD_DATE FROM `{bq_staging_tmp_table_id}` LIMIT 1 ) AND ERROR_MESSAGE IS NOT NULL  ;