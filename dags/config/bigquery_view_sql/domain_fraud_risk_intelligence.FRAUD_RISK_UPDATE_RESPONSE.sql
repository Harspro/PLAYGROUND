SELECT
    OrgId,
    ApiKey,
    TmxRequestId,
    TmxSessionQueryId,
    Action,
    CustomerReportedEventStatus,
    CustomerReportedEventStatusId,
    EventTag,
    CustomUpdate,
    AuthChallengeNumRetries,
    FinalReciveStatus,
    EventClassification,
    INGESTION_TIMESTAMP
FROM `pcb-{env}-landing.domain_fraud_risk_intelligence.FRAUD_RISK_UPDATE_RESPONSE`


