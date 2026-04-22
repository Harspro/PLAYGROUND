SELECT
  customerId,
  initiationToken,
  eftLinkRequestAccountTransactions,
  METADATA,
  INGESTION_TIMESTAMP
FROM
  `pcb-{env}-processing.domain_payments.EFT_LINK_REQUEST_ACCOUNT_TRANSACTIONS_Back_Up`