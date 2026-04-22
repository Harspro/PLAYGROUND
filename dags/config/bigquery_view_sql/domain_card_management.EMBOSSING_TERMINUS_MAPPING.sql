SELECT
    cardEmbossingRequestId,
    cardNumber,
    panSeqNumber,
    fileUuid,
    instantIssuanceOrchestrationId,
    embossingRequestFlow,
    paymentAccountRef,
    pinEnabled,
    metadata.USERVICE_TRACEABILITY_ID,
    INGESTION_TIMESTAMP
FROM `pcb-{env}-landing.domain_card_management.EMBOSSING_TERMINUS_MAPPING`