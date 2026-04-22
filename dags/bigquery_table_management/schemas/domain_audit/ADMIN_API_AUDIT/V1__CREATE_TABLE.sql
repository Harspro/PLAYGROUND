CREATE TABLE IF NOT EXISTS `pcb-{env}-landing.domain_audit.ADMIN_API_AUDIT`
(
  callType STRING,
  action STRING,
  request STRUCT<
    actionDescription STRING,
    requestedBy STRING,
    requestedOn TIMESTAMP,
    referer STRING,
    origin STRING,
    trueClientIp STRING,
    userviceTraceabilityId STRING,
    userviceChannelType STRING,
    groupid STRING,
    userAgent STRING,
    client STRING,
    uri STRING
  >,
  requestData STRING,
  responseData STRING,
  METADATA STRUCT<
    STREAM_NAME STRING,
    TOPIC_PARTITION INT64,
    PARTITION_OFFSET INT64,
    STREAM_OFFSET INT64,
    OFFSET_DIFF INT64,
    ENVIRONMENT STRING,
    TOPIC_VERSION INT64,
    USERVICE_CORRELATION_ID STRING,
    USERVICE_TRACEABILITY_ID STRING
  >,
  INGESTION_TIMESTAMP TIMESTAMP
)
PARTITION BY DATE_TRUNC(INGESTION_TIMESTAMP, MONTH)
CLUSTER BY
  action;