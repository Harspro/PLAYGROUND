CREATE OR REPLACE VIEW
`test_table_id_UNION_TRANSFORM`
OPTIONS (
    expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR),
    description = "View for union transformation"
)
AS (
WITH CIF_TABLE AS (
        SELECT test_column
        , CASE CIFP_ACTION_CODE
            WHEN 'A' THEN 'A'
            WHEN 'C' THEN 'C'
            ELSE 'EMPTY'
        END AS CIFP_ACTION_CODE_SORTING_KEY
        FROM `test_table_id`
        UNION ALL
        SELECT test_column
        , CASE CIFP_ACTION_CODE
            WHEN 'A' THEN 'A'
            WHEN 'C' THEN 'C'
            ELSE 'EMPTY'
        END AS CIFP_ACTION_CODE_SORTING_KEY
        FROM `test_target_table_id`),
    CIF_CURR AS (
        SELECT test_column,
        ROW_NUMBER() OVER(PARTITION BY test_rank_partition ORDER BY test_order_by) AS ROW_RANK
        FROM CIF_TABLE
    )
    SELECT DISTINCT test_column FROM CIF_CURR
    WHERE ROW_RANK = 1
);