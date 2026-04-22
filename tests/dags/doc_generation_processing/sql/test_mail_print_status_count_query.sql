SELECT
COUNT(mps.status) as record_count
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
AND mp.source != 'DMX RRDF';