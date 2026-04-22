SELECT
  customerId AS CUSTOMER_UID,
  eftExternalAccountId AS EFT_EXTERNAL_ACCOUNT_UID,
  institutionNumber AS INSTITUTION_ID,
  transitNumber AS TRANSIT_NUMBER,
  accountNumber AS ACCOUNT_NUMBER,
  nickname AS NICKNAME,
  isDeleted AS DELETED_IND,
  deletedDate AS DELETED_ON,
  verified AS VERIFIED_IND,
  DATETIME(TIMESTAMP(INGESTION_TIMESTAMP), 'America/Toronto') AS RECORD_LOAD_TIMESTAMP,
FROM
  pcb-{env}-landing.domain_payments.EFT_EXTERNAL_ACCOUNT_CHANGED