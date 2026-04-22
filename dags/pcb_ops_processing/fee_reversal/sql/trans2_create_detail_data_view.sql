CREATE OR REPLACE VIEW `{fee.reversal.staging.detail.data.view.id}`
OPTIONS (
    expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR),
    description = "Temp view of detail data"
)
AS
    SELECT
        c.CIFP_ACCOUNT_NUM,
        c.CIFP_ACCOUNT_ID6,
        c.CIFP_LANGUAGE_CODE,
        d.MAST_ACCOUNT_ID,
        SAFE_CAST(d.REFUND AS NUMERIC) AS REFUND,
        d.TRANS_CODE,
        d.BANK_NUMBER,
        d.ESID,
        d.FILE_NAME,
        r.TRANSACTION_TYPE
    FROM `{fee.reversal.staging.input.enriched.data.table.id}` AS d
    JOIN `{cif.active.card.view.id}` AS c
    ON c.CIFP_ACCOUNT_ID6 = d.MAST_ACCOUNT_ID
    LEFT JOIN `{draft5.ref.transaction.code.table.id}` AS r
    ON d.TRANS_CODE = r.TRANSACTION_CODE AND c.CIFP_LANGUAGE_CODE = r.LANGUAGE_CODE
    ORDER BY c.CIFP_ACCOUNT_ID6;
