EXPORT DATA
  OPTIONS (
    uri = 'gs://pcb-{env}-staging-extract/eft-funds-in-rt-process/eft-funds-in-rt-*.parquet',
    format = 'PARQUET',
    overwrite = true
  )
AS
(
SELECT
  TransactionId AS transactionId,
  SettlementTransactionId AS settlementTransactionId,
  ExternalFinancialInstitution AS externalFinancialInstitution,
  ExternalTransitNumber AS externalTransitNumber,
  ExternalAccountNumber AS externalAccountNumber,
  DestinationAccountNumber AS destinationAccountNumber,
  OverallRiskLevel AS overallRiskLevel,
  UserRiskLevel AS userRiskLevel,
  TransactionRiskLevel AS transactionRiskLevel,
  OtherRiskLevel AS otherRiskLevel,
  GuaranteeStatus AS guaranteeStatus,
  UserMatch AS userMatch,
  SAFE_CAST(UNIX_MICROS(SettledOn) AS INT64) AS settledOn,
  SAFE_CAST(UNIX_MICROS(CreatedOn) AS INT64) AS createdOn,
  CounterpartyName AS counterpartyName,
  Type AS type,
  SAFE_CAST(Amount AS STRING) AS amount,
  Currency AS currency,
  CreditOrDebit AS creditOrDebit,
  AccountOrAlias AS accountOrAlias,
  Status AS status,
  SAFE_CAST(UNIX_MICROS(ExecutionDate) AS INT64) AS executionDate,
  SAFE_CAST(UNIX_MICROS(UpdatedOn) AS INT64) AS updatedOn,
  RequestId AS requestId,
  ReferenceId AS referenceId,
  SettlementId AS settlementId,
  CounterpartyEmail AS counterpartyEmail,
  SAFE_CAST(ErrorCode AS STRING) AS errorCode,
  ErrorDescription AS errorDescription
FROM
  `pcb-{env}-processing.domain_payments.PCB_EFT_FUNDS_IN_RT_EXT`
)

