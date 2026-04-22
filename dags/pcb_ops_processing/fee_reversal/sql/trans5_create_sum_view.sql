CREATE OR REPLACE VIEW `{fee.reversal.staging.sum.view.id}`
OPTIONS (
    expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR),
    description = "Temp view of aggregated values"
)
AS
    SELECT
        COUNT(*) AS RECORD_COUNT,
        SUM(REFUND*100) AS NET_AMOUNT,
        SUM(REFUND) AS TOTAL_REFUND,
        MAX(BANK_NUMBER) AS BANK_NUMBER,
        MAX(ESID) AS ESID,
        MAX(FILE_NAME) AS FILE_NAME
    FROM `{fee.reversal.staging.detail.data.view.id}`;