SELECT
  i.eventSpecificationVersion AS EVENT_SPECIFICATION_VERSION,
  i.eventType AS EVENT_TYPE,
  i.eventId AS EVENT_ID,
  i.eventSource AS EVENT_SOURCE,
  i.eventSubject AS EVENT_SUBJECT,
  DATETIME(TIMESTAMP(i.transactDateTime), 'America/Toronto') AS TRANSACT_DATE_TIME,
  DATETIME(TIMESTAMP(id.eventDateTime), 'America/Toronto') AS EVENT_DATE_TIME,
  i.ledgerAccountId AS ACCOUNT_ID,
  i.arrangementId AS ARRANGEMENT_ID,
  ARRAY(
    SELECT AS STRUCT
    ARRAY(
      SELECT AS STRUCT
      amount AS AMOUNT,
      periodStartDate AS PERIOD_START_DATE,
      periodEndDate AS PERIOD_END_DATE
      FROM UNNEST(accrualInfos)) AS ACCRUAL_INFO
  FROM UNNEST(id.interests)) AS INTEREST,
    i.postingDate AS POSTING_DATE,
     DATETIME(TIMESTAMP(i.INGESTION_TIMESTAMP), 'America/Toronto') AS INGESTION_TIMESTAMP
FROM
  pcb-{env}-landing.domain_account_management.TRANSACT_ACCOUNT_INTEREST i
LEFT JOIN
  pcb-{env}-landing.domain_account_management.TRANSACT_ACCOUNT_INTEREST_DETAILS id
ON
  i.arrangementId = id.arrangementId
  AND
  i.postingDate = id.postingDate