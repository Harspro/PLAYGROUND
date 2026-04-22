EXPORT DATA
  OPTIONS ( uri = 'gs://pcb-{env}-staging-extract/tsys-tcs-case-purge-details/tsys-tcs-purge-case-records-*.parquet',
    format = 'PARQUET',
    OVERWRITE = TRUE
  )
AS
WITH
  tsys_purge_case_detail_latest_load AS (
  SELECT
    MAX(cd_ll.rec_load_timestamp) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_dispute.TSYS_TCS_PURGE_CASE_DETAILS` AS cd_ll )
SELECT
  tsys_purge.RECORD_TYPE,
  tsys_purge.PARENT_CASE_ID,
  tsys_purge.CASE_ID,
  tsys_purge.ACCOUNT_ID,
  tsys_purge.WORK_STATUS,
  tsys_purge.FRAUD_INDICATOR,
  tsys_purge.APPLICATION_NAME
FROM
  `pcb-{env}-curated.domain_dispute.TSYS_TCS_PURGE_CASE_DETAILS` tsys_purge
INNER JOIN
  tsys_purge_case_detail_latest_load cd_ll
ON
  cd_ll.LATEST_REC_LOAD_TIMESTAMP = tsys_purge.REC_LOAD_TIMESTAMP;
