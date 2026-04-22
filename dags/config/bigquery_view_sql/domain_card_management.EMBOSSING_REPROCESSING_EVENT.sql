SELECT
    reprocessingEventId,
    status,
    exceptionClass,
    failureReason,
    traceabilityId,
    avroSchema,
    createDt,
    requestId,
    metadata.USERVICE_TRACEABILITY_ID,
    INGESTION_TIMESTAMP
FROM `pcb-{env}-landing.domain_card_management.EMBOSSING_REPROCESSING_EVENT`