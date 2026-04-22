UPDATE `pcb-{env}-landing.domain_fraud_risk_intelligence.FRAUD_RISK_TRUTH_DATA_INBOUND`
SET KafkaPublishStatus = 'COMPLETED'
WHERE upper(KafkaPublishStatus)='PENDING';