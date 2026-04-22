CREATE OR REPLACE TABLE pcb-{env}-processing.domain_account_management.STMT_DAG_HISTORY_TEMP
OPTIONS (
expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR)
)
AS
SELECT
  REPLACE(REPLACE(JSON_VALUE(job_params, '$.cycle_list'),'[',''),']','') AS CYCLES,
  JSON_VALUE(job_params, '$.status') AS STATUS,
  JSON_VALUE(job_params, '$.file_create_dt') AS FILE_CREATE_DT,
  JSON_VALUE(job_params, '$.source_filename') AS FILE_NAME,
  LOAD_TIMESTAMP AS LOAD_TIMESTAMP
FROM
  `pcb-{env}-curated.JobControlDataset.DAGS_HISTORY`
WHERE
  dag_id='pcb_dst_pcmc_stmt_output_file_creation_post_extractor'
AND JSON_VALUE(job_params, '$.file_create_dt') = '{file_create_dt}'
AND JSON_VALUE(job_params, '$.source_filename') like '%{file_name}';