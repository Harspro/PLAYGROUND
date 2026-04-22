SELECT
      customerId,
      pcfCustomerId,
      customerRole,
      accountId,
      parentAccountId,
      ledgerAccountId,
      ledgerAccountType,
      parentLedgerAccountId,
      parentLedgerAccountType,
      productCode,
      accountEventType,
      arrangementId,
      accountOpenDate,
      accountDeactivatedDate,
      INGESTION_TIMESTAMP,
      pcbApplicationReference,
      parentLedgerCustomerId,
      ledgerCustomerId,
      activeInd
FROM pcb-{env}-landing.domain_account_management.ACCOUNT_RELATIONSHIP
QUALIFY ROW_NUMBER() OVER (
              PARTITION BY customerId, accountId, ledgerCustomerId
              ORDER BY INGESTION_TIMESTAMP DESC
    ) = 1