SELECT
    eventId,
    transactionId,
    issuerId,
    requestId,
    domain,
    title,
    occurredOn,
    eventDetail.description as description,
    eventDetail.reasonCode as reasonCode,
    eventDetail.shippingProvider as shippingProvider,
    eventDetail.trackingId as trackingId,
    INGESTION_TIMESTAMP
FROM `pcb-{env}-landing.domain_card_management.CARD_EMBOSSING_REQUEST_STATUS`