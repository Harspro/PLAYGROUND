SELECT
  COALESCE(
    (SELECT o.identification
     FROM UNNEST(IFNULL(FTFCCTI.creditor.identification.privateIdentification.other, [])) AS o
     WHERE o.schemeName.code = 'CUST'
     LIMIT 1),
    (SELECT o.identification
     FROM UNNEST(IFNULL(FTFCCTI.debtor.identification.privateIdentification.other, [])) AS o
     WHERE o.schemeName.code = 'CUST'
     LIMIT 1)
  ) AS CUSTOMER_ID,
  (SELECT envelope.any FROM UNNEST(FMC.fiToFiCustomerCreditTransfer.supplementaryData) WHERE placeAndName='InstructionData.instructionId') AS INSTRUCTION_ID,
  COALESCE(
    (SELECT o.identification
     FROM UNNEST(IFNULL(FTFCCTI.creditor.identification.privateIdentification.other, [])) AS o
     WHERE o.schemeName.proprietary = 'accountId'
     LIMIT 1),
    (SELECT o.identification
     FROM UNNEST(IFNULL(FTFCCTI.debtor.identification.privateIdentification.other, [])) AS o
     WHERE o.schemeName.proprietary = 'accountId'
     LIMIT 1)
  ) AS ACCOUNT_ID,
  DATETIME(TIMESTAMP(FMC.INGESTION_TIMESTAMP), 'America/Toronto') AS RECORD_LOAD_TIMESTAMP
FROM
  pcb-{env}-landing.domain_payments.PCB_FUNDS_MOVE_CREATED AS FMC
CROSS JOIN UNNEST(FMC.fiToFiCustomerCreditTransfer.creditTransferTransactionInformation) AS FTFCCTI
LEFT JOIN
  pcb-{env}-landing.domain_payments.PCB_FUNDS_MOVE_CANCELLED AS FMCA
ON
  (SELECT envelope.any FROM UNNEST(FMC.fiToFiCustomerCreditTransfer.supplementaryData) WHERE placeAndName='InstructionData.instructionId') = FMCA.instructionId
WHERE
  FMCA.instructionId IS NULL;