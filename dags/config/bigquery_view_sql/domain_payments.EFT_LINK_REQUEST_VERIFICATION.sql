SELECT
  customerId AS CUSTOMER_UID,
  initiationToken AS INITIATION_TOKEN,
  externalLoginId AS EXTERNAL_LOGIN_ID,
  requestDate AS REQUEST_DATE,
  status as STATUS,
  institutionNumber AS INSTITUTION_ID,
  transitNumber AS TRANSIT_NUMBER,
  accountNumber AS ACCOUNT_NUMBER,
  nickname AS NICKNAME,
  csrUserId AS CSR_USER_ID,
  errorCode AS ERROR_CODE,
  reasonMessage AS REASON_MESSAGE,
  DATETIME(TIMESTAMP(INGESTION_TIMESTAMP), 'America/Toronto') AS RECORD_LOAD_TIMESTAMP,
FROM
  pcb-{env}-landing.domain_payments.EFT_LINK_REQUEST_VERIFICATION