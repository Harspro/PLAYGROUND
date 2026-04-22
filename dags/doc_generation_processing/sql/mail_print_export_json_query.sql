EXPORT DATA OPTIONS (
    uri='gs://pcb-{env}-doc-generation-outputs/{unique_folder_path}/data/mail_print_export-*.json',
    format='JSON',
    overwrite=true )
AS (
    SELECT
        language,
        communicationCode as code,
        uuid,
        mailType,
        printingMode,
        parameters
    FROM `pcb-{env}-landing.domain_communication.MAIL_PRINT` mp
    WHERE mp.INGESTION_TIMESTAMP >= TIMESTAMP('{start_time}', '{est_zone}')
        AND mp.INGESTION_TIMESTAMP < TIMESTAMP('{end_time}', '{est_zone}')
        AND NOT EXISTS (
            SELECT 1
            FROM `pcb-{env}-landing.domain_communication.MAIL_PRINT_STATUS` mps
            WHERE mps.communicationCode = mp.communicationCode
                AND mps.uuid = mp.uuid
                AND mps.status = 'AFP_GENERATED'
        )
);