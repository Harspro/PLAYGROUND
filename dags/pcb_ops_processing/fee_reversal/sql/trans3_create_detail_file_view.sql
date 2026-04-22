CREATE OR REPLACE VIEW `{fee.reversal.staging.detail.file.view.id}`
OPTIONS (
    expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR),
    description = "Temp view of detail data aligned with draft5 layout"
)
AS
    SELECT
        LPAD(SAFE_CAST((ROW_NUMBER() OVER (ORDER BY CIFP_ACCOUNT_ID6 ASC))+2 AS STRING), 7, '0') AS SEQUENCE_NUMBER,  -- add 2 for transmit header and batch header
        LPAD(SAFE_CAST(TRANS_CODE AS STRING), 4, '0') AS TRANSACTION_CODE,
        RPAD(CIFP_ACCOUNT_NUM, 16) AS ACCOUNT_NUMBER,
        FORMAT_DATE('%m%d%y', CURRENT_DATE('America/Toronto')) AS TRANSACTION_DATE,
        ' ' AS TANDEM_AUTH_FLAG,
        LPAD(SAFE_CAST(REFUND*100 AS STRING), 9, '0') AS TRANSACTION_AMOUNT,
        RPAD(TRANSACTION_TYPE, 25) AS MERCHANT_NAME,
        RPAD(' ', 13) AS MERCHANT_CITY,
        RPAD(' ', 3) AS MERCHANT_STATE_PROV,
        'Y' AS ALLOW_MER_DESC_ON_CORR,
        RPAD(' ', 11) AS REFERENCE_NUMBER,
        RPAD(' ', 12) AS REFERENCE_NUMBER_EXT,
        RPAD(' ', 4) AS FILLER1,
        RPAD(' ', 3) AS MERCHANT_COUNTRY_CODE,
        '0000' AS MERCHANT_CATEGORY_CODE,
        ' ' AS FILLER2,
        RPAD(' ', 6) AS AUTHORIZATION_CODE,
        ' ' AS MAIL_PHONE_ORDER,
        ' ' AS EXTENSION_IND,
        CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
        FILE_NAME
    FROM `{fee.reversal.staging.detail.data.view.id}`;