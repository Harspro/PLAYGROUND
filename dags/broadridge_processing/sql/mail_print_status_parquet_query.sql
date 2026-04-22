EXPORT DATA OPTIONS (
    uri = 'gs://{parquet_file_path}',
    format = 'PARQUET',
    OVERWRITE = true )
    AS (
        SELECT
            mps.communicationCode,
            mps.uuid,
            mps.status,
            mps.outboundFileName,
            mps.dagId,
            mps.dagRunId,
            mps.REC_CREATE_TIMESTAMP
        FROM
            `pcb-{env}-landing.domain_communication.MAIL_PRINT_STATUS` mps
        INNER JOIN
            `pcb-{env}-landing.domain_communication.MAIL_PRINT` mp
            ON mps.communicationCode = mp.communicationCode
                AND mps.uuid = mp.uuid
        WHERE
            mps.outboundFileName = '{outbound_file_name}'
            AND mps.dagId = '{dag_id}'
            AND mps.dagRunId = '{dag_run_id}'
            AND mps.status = UPPER('MAILED')
            -- filter out statuses related to DMX RRDF flow as records for those do not exist in DP
            AND mp.source != 'DMX RRDF'
    );