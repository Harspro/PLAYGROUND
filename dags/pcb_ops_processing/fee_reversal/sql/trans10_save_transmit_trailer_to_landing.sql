CREATE TABLE IF NOT EXISTS `{fee.reversal.landing.transmit.trailer.table.id}`
AS
    SELECT
        SEQUENCE_NUMBER,
        TRANSACTION_CODE,
        TRANSMIT_BANK_NUMBER,
        USER_SEQUENCE_NUMBER,
        TOTAL_AMOUNT_OF_DEBITS,
        TOTAL_AMOUNT_OF_CREDITS,
        TOTAL_AMOUNT_OF_PAYMENTS,
        FILLER,
        REC_LOAD_TIMESTAMP,
        FILE_NAME
    FROM `{fee.reversal.staging.transmit.trailer.file.view.id}`
    LIMIT 0;

INSERT INTO `{fee.reversal.landing.transmit.trailer.table.id}`
    SELECT
        SEQUENCE_NUMBER,
        TRANSACTION_CODE,
        TRANSMIT_BANK_NUMBER,
        USER_SEQUENCE_NUMBER,
        TOTAL_AMOUNT_OF_DEBITS,
        TOTAL_AMOUNT_OF_CREDITS,
        TOTAL_AMOUNT_OF_PAYMENTS,
        FILLER,
        REC_LOAD_TIMESTAMP,
        FILE_NAME
    FROM `{fee.reversal.staging.transmit.trailer.file.view.id}`;