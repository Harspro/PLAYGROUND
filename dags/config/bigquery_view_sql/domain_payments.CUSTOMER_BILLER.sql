SELECT
  customerId AS CustomerId,
  customerBillerId AS CustomerBillerId,
  billerNumber AS BillerNumber,
  billerAccountNumber AS BillerAccountNumber,
  nickname AS NICKNAME,
  isDeleted AS Deleted,
  DATETIME(TIMESTAMP(INGESTION_TIMESTAMP), 'America/Toronto') AS RECORD_LOAD_TIMESTAMP,
FROM
  pcb-{env}-landing.domain_payments.CUSTOMER_BILLER_CHANGED
