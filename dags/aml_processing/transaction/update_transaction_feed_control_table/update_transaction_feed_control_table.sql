CREATE
OR REPLACE TABLE `pcb-{env}-processing.domain_aml.TRANSACTION_FEED_CONTROL_TABLE_BACK_UP` OPTIONS (
    expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
) AS
SELECT
    *
FROM
    `pcb-{env}-processing.domain_aml.TRANSACTION_FEED_CONTROL_TABLE`;

CREATE
OR REPLACE TABLE `pcb-{env}-processing.domain_aml.TRANSACTION_FEED_CONTROL_TABLE` AS
SELECT
    CT.*,
    COALESCE(HRT.RECORDS_COUNT, 0) AS RECORDS_COUNT
FROM
    `pcb-{env}-processing.domain_aml.TRANSACTION_FEED_CONTROL_TABLE` CT
    LEFT JOIN (
        SELECT
            RUN_ID,
            COUNT(*) AS RECORDS_COUNT
        FROM
            `pcb-{env}-curated.cots_aml_verafin.TRANSACTION_FEED_HISTORICAL_RECORDS_TABLE`
        GROUP BY
            RUN_ID
    ) AS HRT ON CT.RUN_ID = HRT.RUN_ID