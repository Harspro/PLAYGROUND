CREATE OR REPLACE TABLE
  `{staging_table_id}`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
) AS
SELECT {columns}
FROM `{source_table_id}`
LIMIT 0;
INSERT INTO `{staging_table_id}` ({columns})
SELECT {columns}
FROM `{source_table_id}`;


