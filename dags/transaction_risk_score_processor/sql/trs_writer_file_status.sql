CREATE OR REPLACE TABLE `pcb-{env}-curated.domain_scoring.trs_writer_file_status_temp` (
read_me STRING 
) OPTIONS (
    expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL 10 MINUTE),
    description = 'This table is use by TRS Writer vendor file status SQL file.' 
    
    );

INSERT INTO `pcb-{env}-curated.domain_scoring.trs_writer_file_status_temp` values ('dummy Record');

EXPORT DATA
  OPTIONS (
    uri = 'gs://pcb-{env}-staging-extract/trs-vendor-file-status/trs-vendor-file-status-*.parquet',
    format = 'PARQUET',
    overwrite = true
  )
AS
(
SELECT
CAST(null AS STRING) as vendor,
CAST('{execution_id}' AS STRING)  as executionId,
CAST('{file_name}' AS STRING)  as fileName,
CAST(null AS STRING)  as dagName,
CAST(null AS STRING)  as records,
CAST('{file_status}' AS STRING)  as status
FROM `pcb-{env}-curated.domain_scoring.trs_writer_file_status_temp`
);