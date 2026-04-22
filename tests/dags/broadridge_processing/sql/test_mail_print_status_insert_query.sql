INSERT INTO `pcb-{env}-landing.domain_communication.MAIL_PRINT_STATUS` (communicationCode, uuid, status, outboundFileName, dagId, dagRunId)
SELECT
communicationCode,
uuid,
'MAILED',
'<<OUTBOUND_FILE_NAME>>',
'<<DAG_ID>>',
'<<DAG_RUN_ID>>'
FROM
`pcb-{env}-landing.domain_communication.MAIL_PRINT_STATUS`
WHERE
outboundFileName = '<<OUTBOUND_FILE_NAME>>'
AND status = UPPER('AFP_GENERATED');