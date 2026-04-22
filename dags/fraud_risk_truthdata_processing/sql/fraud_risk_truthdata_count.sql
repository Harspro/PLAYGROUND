SELECT count(RequestId) AS truthdata_count
FROM `pcb-{env}-landing.domain_fraud_risk_intelligence.FRAUD_RISK_TRUTH_DATA_INBOUND`
WHERE upper(KafkaPublishStatus)='PENDING';