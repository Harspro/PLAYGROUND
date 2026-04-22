EXPORT DATA OPTIONS (
  uri = 'gs://pcb-{env}-staging-extract/all_score_status/status-*.parquet',
  format = 'PARQUET',
  overwrite = true
) AS (
  SELECT
    CAST('{score_type}' AS STRING) as SCORE_TYPE,
    CAST(EXECUTION_ID AS STRING) as EXECUTION_ID,
    CAST('{file_name}' AS STRING) as FILE_NAME,
    'all_score_status_kafka_writer' as DAG_NAME,
    CAST(COUNT(1) AS STRING) AS RECORDS,
    CAST('{file_status}' AS STRING) as STATUS
  FROM
    `pcb-{env}-curated.domain_scoring.{all_score_output_table}`
  GROUP BY
    EXECUTION_ID
);