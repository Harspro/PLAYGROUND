CREATE TABLE IF NOT EXISTS {bq_history_table_id} ({staging_table_schema});

INSERT INTO {bq_history_table_id}
SELECT * FROM {bq_staging_tmp_table_id} WHERE TRIM(LOWER(ACTION)) <> 'ignore';