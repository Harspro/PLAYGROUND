CREATE OR REPLACE VIEW `{fee.reversal.staging.batch.trailer.file.view.id}`
OPTIONS (
    expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR),
    description = "Temp view of batch trailer aligned with draft5 layout"
)
AS
    SELECT
        LPAD(SAFE_CAST(d.RECORD_COUNT+3 AS STRING), 7, '0') AS SEQUENCE_NUMBER, -- add 3 for transmit header, batch header, detail records
        '9013' AS TRANSACTION_CODE,
        d.BANK_NUMBER AS TRANSMIT_BANK_NUMBER,
        'DRFT5' AS BATCH_TYPE_DESCRIPTION,
        FORMAT_DATE('%j', CURRENT_DATE('America/Toronto')) AS JULIAN_DATE,
        '0001' AS BATCH_DATA,
        RPAD(' ', 3) AS BATCH_OPERATOR,
        CONCAT(LEFT(LPAD(CAST(CAST(d.NET_AMOUNT AS INT64) AS STRING), 12, '0'), 11),h.ALTERNATE_AMT_FLAG) AS NET_AMT_OF_BATCH,
        '000000' AS NUMBER_OF_DEBITS,
        RPAD(d.ESID, 10, '0') AS AMOUNT_OF_DEBITS,
        LPAD(SAFE_CAST(d.RECORD_COUNT AS STRING), 6, '0') AS NUMBER_OF_CREDITS,
        LPAD(SAFE_CAST(d.NET_AMOUNT AS STRING), 10, '0') AS AMOUNT_OF_CREDITS,
        '000000' AS NUMBER_OF_PAYMENTS,
        '0000000000' AS AMOUNT_OF_PAYMENTS,
        RPAD(' ', 38) AS FILLER,
        CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
        d.FILE_NAME
    FROM `{fee.reversal.staging.trailer.data.view.id}` d
    JOIN `{draft5.ref.hex.chart.table.id}` h
    ON d.LAST_DIGIT=h.AMOUNT_LAST_DIGIT
    AND h.AMOUNT_TYPE='Credit';