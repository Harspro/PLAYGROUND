CREATE OR REPLACE VIEW `{cif.active.card.view.id}`
OPTIONS (
    expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR),
    description = "Temp view of CIF active cards"
)
AS
    SELECT
        CIFP_ACCOUNT_NUM,
        CIFP_ACCOUNT_ID6,
        COALESCE(NULLIF(TRIM(CIFP_LANGUAGE_CODE), ''), 'ENU') AS CIFP_LANGUAGE_CODE
    FROM `{cif.card.curr.view.id}`
    WHERE CIFP_CUSTOMER_TYPE = 0
        AND COALESCE(NULLIF(TRIM(CIFP_RELATIONSHIP_STAT), ''), 'A') = 'A';
