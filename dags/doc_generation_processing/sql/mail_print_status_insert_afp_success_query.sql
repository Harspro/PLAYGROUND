INSERT INTO `pcb-{env}-landing.domain_communication.MAIL_PRINT_STATUS` (communicationCode, uuid, status, outboundFileName, dagId, dagRunId)
    SELECT
        communicationCode,
        uuid,
        'AFP_GENERATED',
        '{outbound_filename}',
        '{dag_id}',
        '{dag_run_id}'
    FROM
        `pcb-{env}-landing.domain_communication.MAIL_PRINT_STATUS`
    WHERE
        path = '{path}'
        AND dagId = '{dag_id}'
        AND dagRunId = '{dag_run_id}'
        AND status = 'PDF_GENERATED';