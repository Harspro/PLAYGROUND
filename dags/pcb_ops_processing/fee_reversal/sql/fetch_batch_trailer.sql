-- query used in file generation, column orders need to follow the copybook
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
    FILLER
FROM `{fee.reversal.staging.batch.trailer.file.view.id}`;