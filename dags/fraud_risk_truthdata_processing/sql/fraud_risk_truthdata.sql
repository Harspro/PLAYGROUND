EXPORT DATA
OPTIONS (
  uri = 'gs://pcb-{env}-staging-extract/fraud_risk_truthdata/fraud-risk-truthdata-*.parquet',
  format = 'PARQUET',
  overwrite = true
)
AS
SELECT
  'fraud_application' AS CONTENT_TYPE,
  RequestId AS REQUEST_ID,
  Action AS ACTION,
  FinalReviewStatus AS FINAL_REVIEW_STATUS,
  EventClassification AS EVENT_CLASSIFICATION,
  EventTag AS EVENT_TAG,
  GENERATE_UUID() AS TRUTHDATAUID
FROM `pcb-{env}-landing.domain_fraud_risk_intelligence.FRAUD_RISK_TRUTH_DATA_INBOUND`
WHERE upper(KafkaPublishStatus)='PENDING';
