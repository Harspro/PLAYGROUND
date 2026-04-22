CREATE TABLE IF NOT EXISTS `{bq_audit_table_id}` ({audit_table_schema})
PARTITION BY DATE(AUDIT_DATE);

INSERT INTO `{bq_audit_table_id}`
SELECT 
    (SELECT LOAD_DATE FROM `{bq_staging_tmp_table_id}` LIMIT 1) AS AUDIT_DATE,
     '{reference_table_id_parameter}' AS REFERENCE_TABLE_ID,
    (SELECT COUNT(*) FROM `{bq_staging_tmp_table_id}`) AS STAGING_RECORDS,
    (SELECT COUNT(*) FROM `{bq_target_table_id}` WHERE LOAD_DATE = (SELECT LOAD_DATE FROM `{bq_staging_tmp_table_id}` LIMIT 1)) AS INGESTED_INTO_TARGET,
    (SELECT COUNT(*) FROM {bq_invalid_table_id} WHERE LOAD_DATE = (SELECT LOAD_DATE FROM `{bq_staging_tmp_table_id}` LIMIT 1) AND ERROR_MESSAGE IS NOT NULL ) AS INVALID_RECORDS,
    (
        (SELECT COUNT(*) FROM `{bq_target_table_id}` WHERE LOAD_DATE = (SELECT LOAD_DATE FROM `{bq_staging_tmp_table_id}` LIMIT 1)) +
        (SELECT COUNT(*) FROM {bq_invalid_table_id} WHERE LOAD_DATE = (SELECT LOAD_DATE FROM `{bq_staging_tmp_table_id}` LIMIT 1) AND ERROR_MESSAGE IS NOT NULL )
    ) AS TOTAL_PROCESSED,
    CASE 
        WHEN (
            (SELECT COUNT(*) FROM `{bq_target_table_id}` WHERE LOAD_DATE = (SELECT LOAD_DATE FROM `{bq_staging_tmp_table_id}` LIMIT 1)) +
            (SELECT COUNT(*) FROM {bq_invalid_table_id} WHERE LOAD_DATE = (SELECT LOAD_DATE FROM `{bq_staging_tmp_table_id}` LIMIT 1) AND ERROR_MESSAGE IS NOT NULL )
        ) = (SELECT COUNT(*) FROM `{bq_staging_tmp_table_id}`)
        THEN 'All records processed'  
        ELSE 'Some records failed'  
    END AS STATUS;