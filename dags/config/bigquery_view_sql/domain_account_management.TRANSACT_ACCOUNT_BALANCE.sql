SELECT
  eventSpecificationVersion AS EVENT_SPECIFICATION_VERSION,
  eventType AS EVENT_TYPE,
  eventId AS EVENT_ID,
  eventSource AS EVENT_SOURCE,
  eventSubject AS EVENT_SUBJECT,
  DATETIME(TIMESTAMP(transactDateTime), 'America/Toronto') AS TRANSACT_DATE_TIME,
  DATETIME(TIMESTAMP(eventDateTime), 'America/Toronto') AS EVENT_DATE_TIME,
  ledgerAccountId AS ACCOUNT_ID,
  arrangementId AS ARRANGEMENT_ID,
  balanceName AS BALANCE_NAME,
  balanceDateType AS BALANCE_DATE_TYPE,
  balanceClosing AS BALANCE_CURRENT,
  balanceOpening AS BALANCE_LAST_COB,
  postingDate AS POSTING_DATE,
  DATETIME(TIMESTAMP(INGESTION_TIMESTAMP), 'America/Toronto') AS INGESTION_TIMESTAMP
FROM
  pcb-{env}-landing.domain_account_management.TRANSACT_ACCOUNT_BALANCE