EXPORT DATA OPTIONS (
uri = 'gs://pcb-{env}-staging-extract/document_generation/{dag_id}/{unique_folder_path}/mail-print-status-*.parquet',
format = 'PARQUET',
OVERWRITE = true )
AS (
SELECT
mps.communicationCode,
mps.uuid,
mps.status,
mps.outboundFileName,
mps.path,
mps.dagId,
mps.dagRunId,
mps.REC_CREATE_TIMESTAMP
FROM
`pcb-{env}-landing.domain_communication.MAIL_PRINT_STATUS` mps
INNER JOIN
`pcb-{env}-landing.domain_communication.MAIL_PRINT` mp
ON mp.communicationCode = mps.communicationCode
AND mp.uuid = mps.uuid
WHERE
mps.dagId = '{dag_id}'
AND mps.dagRunId = '{dag_run_id}'
-- filter out statuses related to DMX RRDF flow as records for those do not exist in DP
AND mp.source != 'DMX RRDF'
);