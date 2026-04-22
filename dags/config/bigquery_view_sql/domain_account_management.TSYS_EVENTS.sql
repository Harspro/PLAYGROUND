SELECT
  eventCorrelationId,
  eventType,
  requestType,
  batchEvent,
  payload,
  consumedTime,
  eventTime,
  ingestion_timestamp as ingestionTimestamp,
  externalTopicName,
  externalPartition,
  externalOffset,
  messageTimestamp
FROM
  `pcb-{env}-landing.domain_account_management.TSYS_EVENTS`