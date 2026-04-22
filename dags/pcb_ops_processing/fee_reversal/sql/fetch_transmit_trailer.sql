-- query used in file generation, column orders need to follow the copybook
SELECT
    SEQUENCE_NUMBER,
    TRANSACTION_CODE,
    TRANSMIT_BANK_NUMBER,
    USER_SEQUENCE_NUMBER,
    TOTAL_AMOUNT_OF_DEBITS,
    TOTAL_AMOUNT_OF_CREDITS,
    TOTAL_AMOUNT_OF_PAYMENTS,
    FILLER
FROM `{fee.reversal.staging.transmit.trailer.file.view.id}`;