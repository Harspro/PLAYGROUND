SELECT
    orchestrationId,
    orchestrationEventName,
    cardNumber,
    panSeqNumber,
    createDate,
    reasonCode,
    reasonDescription,
    metadata.USERVICE_TRACEABILITY_ID,
    INGESTION_TIMESTAMP
FROM `pcb-{env}-landing.domain_card_management.INSTANT_ISSUANCE_ORCHESTRATION`