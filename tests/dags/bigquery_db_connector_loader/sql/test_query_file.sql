CREATE OR REPLACE TABLE
`{staging_table_id}`
OPTIONS (
expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
) AS (SELECT * FROM `pcb-{env}-landing.domain_test.test_table`);