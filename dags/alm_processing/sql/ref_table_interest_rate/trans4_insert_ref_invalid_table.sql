CREATE TABLE IF NOT EXISTS `{bq_invalid_table_id}` ({invalid_table_schema})
PARTITION BY DATE(LOAD_DATE);


INSERT INTO `{bq_invalid_table_id}`
SELECT * FROM `{bq_invalid_tmp_table_id}`;