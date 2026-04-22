INSERT INTO `pcb-{env}-landing.domain_loyalty.PC_TARGETED_OFFER_FULFILLMENT`
SELECT
  S.offerFulfillmentId,
  S.accountId ,
  S.tsysAccountId ,
  S.offerTriggerId ,
  IF(S.periodStartTimestamp IS NULL,TIMESTAMP('2025-09-02T20:00:00'),PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S',  S.periodStartTimestamp)) AS periodStartTimestamp,
  IF(S.periodEndTimestamp IS NULL,TIMESTAMP('2025-12-02T19:00:00'),PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S',  S.periodEndTimestamp)) AS periodEndTimestamp,
  IF(S.trackerType IS NULL, 'PCHCNT', S.trackerType ) AS trackerType,
  IF(S.status IS NULL, 'OP', S.status ) AS status,
  IF(S.currentValue IS NULL, '0', S.currentValue ) AS currentValue,
  IF(S.targetValue IS NULL, '1', S.targetValue ) AS targetValue,
  IF(S.points IS NULL, 0 , S.points ) AS points,
  PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', NULLIF(S.fulfilledTimestamp, 'null')) AS fulfilledTimestamp,
  S.fulfilledTransactionId ,
  CURRENT_TIMESTAMP() AS createTimestamp,
  IF(S.createUserId IS NULL,'MANUAL_UPDATE' , S.createUserId ) AS createUserId,
  PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', NULLIF(S.updateTimestamp, 'null')) AS updateTimestamp,
  S.updateUserId AS updateUserId ,
  S.eventType AS eventType,
  CAST(NULL AS STRUCT<
    STREAM_NAME	STRING,
    TOPIC_PARTITION	INTEGER,
    PARTITION_OFFSET	INTEGER,
    STREAM_OFFSET	INTEGER,
    OFFSET_DIFF	INTEGER,
    ENVIRONMENT	STRING,
    TOPIC_VERSION	INTEGER,
    USERVICE_CORRELATION_ID	STRING,
    USERVICE_TRACEABILITY_ID	STRING
  >) AS METADATA,
  CURRENT_TIMESTAMP() as INGESTION_TIMESTAMP
FROM `pcb-{env}-processing.domain_loyalty.PC_TARGETED_OFFER_FULFILLMENT_EXT` S
LEFT JOIN `pcb-{env}-landing.domain_loyalty.PC_TARGETED_OFFER_FULFILLMENT` T
ON S.tsysAccountId = T.tsysAccountId
WHERE T.tsysAccountId IS NULL
AND UPPER(S.updateType)='INSERT'