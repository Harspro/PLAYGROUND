SELECT
  FMCA.customerId                     AS CUSTOMER_ID,
  FMCA.instructionId                  AS INSTRUCTION_ID,
  FMCA.fundsMoveType                  AS FUNDS_MOVE_TYPE,
  FMCA.cancelledDate                  AS CANCELLED_DATE,
  FMCA.cancelledReasonCode            AS CANCELLED_REASON_CODE,
  DATETIME(TIMESTAMP(FMCA.INGESTION_TIMESTAMP), 'America/Toronto') AS RECORD_LOAD_TIMESTAMP
FROM
  pcb-{env}-landing.domain_payments.PCB_FUNDS_MOVE_CANCELLED AS FMCA
LEFT JOIN (
  SELECT
    (SELECT envelope.any FROM UNNEST(FMC.fiToFiCustomerCreditTransfer.supplementaryData) WHERE placeAndName='InstructionData.instructionId') AS instructionIdentification
  FROM
     pcb-{env}-landing.domain_payments.PCB_FUNDS_MOVE_CREATED AS FMC
) AS FMC_UNNESTED
  ON FMC_UNNESTED.instructionIdentification = FMCA.instructionId;
