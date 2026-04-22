SELECT
reprocessingEventId,
status,
failureReason,
reprocessingCount,
lastReprocessingDate,
recordType,
topicName,
groupId,
applicationName,
metadata.uservice_traceability_id as userviceTraceabilityId,
ingestion_timestamp as ingestionTimestamp
FROM `pcb-{env}-landing.domain_technical.REPROCESSING_EVENT`