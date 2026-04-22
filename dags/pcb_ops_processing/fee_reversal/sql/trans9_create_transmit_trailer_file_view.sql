CREATE OR REPLACE VIEW `{fee.reversal.staging.transmit.trailer.file.view.id}`
OPTIONS (
    expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR),
    description = "Temp view of batch trailer aligned with draft5 layout"
)
AS
    SELECT
        LPAD(SAFE_CAST(RECORD_COUNT+4 AS STRING), 7, '0') AS SEQUENCE_NUMBER, -- add 4 for transmit header, batch header, detail records, batch trailer
        '9011' AS TRANSACTION_CODE,
        BANK_NUMBER AS TRANSMIT_BANK_NUMBER,
        '0001' AS USER_SEQUENCE_NUMBER,
        '000000000000' AS TOTAL_AMOUNT_OF_DEBITS,
        LPAD(SAFE_CAST(NET_AMOUNT AS STRING), 12, '0') AS TOTAL_AMOUNT_OF_CREDITS,
        '000000000000' AS TOTAL_AMOUNT_OF_PAYMENTS,
        RPAD(' ', 73) AS FILLER,
        CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
        FILE_NAME
    FROM `{fee.reversal.staging.trailer.data.view.id}`;