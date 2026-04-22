CREATE TABLE IF NOT EXISTS `{bq_target_table_id}` ({target_table_schema})
PARTITION BY DATE(LOAD_DATE);

CREATE OR REPLACE TABLE `{bq_invalid_tmp_table_id}`
OPTIONS (
    expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR),
    description = "Temporary table storing validation errors before final processing"
)
AS
WITH latest_records AS (
    SELECT 
        VENDOR_CODE, INSTRUMENT_TYPE, FIELD_NAME, BANK, RATE, EFFECTIVE_DATE, END_DATE, LOAD_DATE
    FROM `{bq_target_table_id}`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY VENDOR_CODE, INSTRUMENT_TYPE, FIELD_NAME, BANK ORDER BY LOAD_DATE DESC) = 1
),
validation_checks AS (
    SELECT 
        stg.*,
        CASE 
            WHEN LOWER(SPLIT(TRIM(stg.REQUESTOR_EMAIL), '@')[SAFE_OFFSET(1)]) != "pcbank.ca"
            THEN 'Invalid email domain'

            WHEN TRIM(UPPER(stg.REFERENCE_TABLE_ID)) != '{reference_table_id_parameter}'
            THEN 'Invalid reference table ID'

            WHEN TRIM(LOWER(stg.ACTION)) NOT IN ('add', 'change', 'expire')
            THEN 'Invalid ACTION code: Must be add, change, or expire'

            WHEN (TRIM(LOWER(stg.ACTION)) = 'add' AND 
                  (stg.VENDOR_CODE IS NULL OR stg.INSTRUMENT_TYPE IS NULL OR stg.FIELD_NAME IS NULL OR stg.BANK IS NULL OR 
                   stg.EFFECTIVE_DATE IS NULL OR stg.NEW_RATE IS NULL))
            THEN 'Missing required fields for ADD action'

            WHEN TRIM(LOWER(stg.ACTION)) = 'add'
                AND latest.VENDOR_CODE IS NOT NULL 
                AND latest.INSTRUMENT_TYPE IS NOT NULL 
                AND latest.FIELD_NAME IS NOT NULL
                AND latest.BANK IS NOT NULL
            THEN 'Duplicate record already exists in target table for ADD action'

            WHEN (TRIM(LOWER(stg.ACTION)) = 'change' AND 
                  (stg.VENDOR_CODE IS NULL OR stg.INSTRUMENT_TYPE IS NULL OR stg.FIELD_NAME IS NULL OR stg.BANK IS NULL OR 
                   stg.EFFECTIVE_DATE IS NULL OR stg.OLD_RATE IS NULL OR stg.NEW_RATE IS NULL))
            THEN 'Missing required fields for CHANGE action'

            WHEN TRIM(LOWER(stg.ACTION)) = 'change'
                AND latest.VENDOR_CODE IS NULL
                AND latest.INSTRUMENT_TYPE IS NULL 
                AND latest.FIELD_NAME IS NULL
                AND latest.BANK IS NULL
            THEN 'Record not found in target table for CHANGE action'

            WHEN (TRIM(LOWER(stg.ACTION)) = 'change' 
                AND latest.VENDOR_CODE = stg.VENDOR_CODE 
                AND latest.INSTRUMENT_TYPE = stg.INSTRUMENT_TYPE 
                AND latest.FIELD_NAME = stg.FIELD_NAME 
                AND latest.BANK = stg.BANK
                AND latest.RATE != 
                    CASE 
                        WHEN SAFE_CAST(stg.OLD_RATE AS FLOAT64) IS NOT NULL 
                        THEN FORMAT("%.3f", SAFE_CAST(stg.OLD_RATE AS FLOAT64))  
                        ELSE LOWER(stg.OLD_RATE)  
                    END
            )
            THEN 'Old rate does not match latest record for CHANGE action'

            WHEN (TRIM(LOWER(stg.ACTION)) = 'expire' 
                AND (
                    latest.VENDOR_CODE IS NULL 
                    OR latest.INSTRUMENT_TYPE IS NULL 
                    OR latest.FIELD_NAME IS NULL
                    OR latest.BANK IS NULL
                )
            )
            THEN 'Record not found in target table for EXPIRE action'

            WHEN (TRIM(LOWER(stg.ACTION)) = 'expire'
                AND latest.VENDOR_CODE = stg.VENDOR_CODE
                AND latest.INSTRUMENT_TYPE = stg.INSTRUMENT_TYPE
                AND latest.FIELD_NAME = stg.FIELD_NAME
                AND latest.BANK = stg.BANK
                AND latest.EFFECTIVE_DATE = stg.EFFECTIVE_DATE
                AND (latest.END_DATE IS NOT NULL AND latest.END_DATE <= CURRENT_DATE())
            )
            THEN 'Record is already expired in target table'


            WHEN stg.END_DATE < stg.EFFECTIVE_DATE 
            THEN 'Invalid END_DATE: Must be greater than or equal to EFFECTIVE_DATE'

            ELSE NULL  
        END AS ERROR_MESSAGE
    FROM `{bq_staging_tmp_table_id}` stg
    LEFT JOIN latest_records latest
        ON TRIM(LOWER(stg.VENDOR_CODE)) = latest.VENDOR_CODE
        AND TRIM(LOWER(stg.INSTRUMENT_TYPE)) = latest.INSTRUMENT_TYPE
        AND TRIM(LOWER(stg.FIELD_NAME)) = latest.FIELD_NAME
        AND TRIM(LOWER(stg.BANK)) = latest.BANK
        WHERE TRIM(LOWER(stg.ACTION)) <> 'ignore'
)
SELECT * FROM validation_checks;
