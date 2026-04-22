CREATE TABLE IF NOT EXISTS `{fee.reversal.landing.batch.trailer.table.id}`
AS
    SELECT
        SEQUENCE_NUMBER,
        TRANSACTION_CODE,
        TRANSMIT_BANK_NUMBER,
        BATCH_TYPE_DESCRIPTION,
        JULIAN_DATE,
        BATCH_DATA,
        BATCH_OPERATOR,
        NET_AMT_OF_BATCH,
        NUMBER_OF_DEBITS,
        AMOUNT_OF_DEBITS,
        NUMBER_OF_CREDITS,
        AMOUNT_OF_CREDITS,
        NUMBER_OF_PAYMENTS,
        AMOUNT_OF_PAYMENTS,
        FILLER,
        REC_LOAD_TIMESTAMP,
        FILE_NAME
    FROM `{fee.reversal.staging.batch.trailer.file.view.id}`
    LIMIT 0;

INSERT INTO `{fee.reversal.landing.batch.trailer.table.id}`
    SELECT
        SEQUENCE_NUMBER,
        TRANSACTION_CODE,
        TRANSMIT_BANK_NUMBER,
        BATCH_TYPE_DESCRIPTION,
        JULIAN_DATE,
        BATCH_DATA,
        BATCH_OPERATOR,
        NET_AMT_OF_BATCH,
        NUMBER_OF_DEBITS,
        AMOUNT_OF_DEBITS,
        NUMBER_OF_CREDITS,
        AMOUNT_OF_CREDITS,
        NUMBER_OF_PAYMENTS,
        AMOUNT_OF_PAYMENTS,
        FILLER,
        REC_LOAD_TIMESTAMP,
        FILE_NAME
    FROM `{fee.reversal.staging.batch.trailer.file.view.id}`;