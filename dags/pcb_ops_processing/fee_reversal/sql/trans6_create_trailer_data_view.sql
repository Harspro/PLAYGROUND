CREATE OR REPLACE VIEW `{fee.reversal.staging.trailer.data.view.id}`
OPTIONS (
    expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR),
    description = "Temp view of trailer data"
)
AS
    SELECT
        RECORD_COUNT,
        NET_AMOUNT,
        TOTAL_REFUND,
        BANK_NUMBER,
        ESID,
        FILE_NAME,
        SAFE_CAST(SUBSTR(LPAD(SAFE_CAST(NET_AMOUNT AS STRING), 12, '0'), 12, 1) AS INT64) AS LAST_DIGIT
    FROM `{fee.reversal.staging.sum.view.id}`;