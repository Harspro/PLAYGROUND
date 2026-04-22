INSERT INTO
    `{TRANSACTION_HISTORY_TABLE}` (
        {column_list_transaction_feed},
        RUN_ID,
        RAIL_TYPE,
        FILENAME,
        FILE_SENT_DATE
    )
SELECT
    {column_list_historical_data},
    '{run_id}' AS RUN_ID,
    '{rail_type}' AS RAIL_TYPE,
    {updated_filename} AS FILENAME,
    CURRENT_DATE('America/Toronto') AS FILE_SENT_DATE
FROM
    `{transaction_feed_table}`;