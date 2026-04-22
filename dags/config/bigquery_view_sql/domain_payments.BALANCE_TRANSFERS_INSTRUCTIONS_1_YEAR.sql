WITH FUNDSMOVE_INSTRUCTION AS (
  SELECT
    *
  FROM
    pcb-{env}-landing.domain_payments.FUNDSMOVE_INSTRUCTION
  QUALIFY ROW_NUMBER() OVER (PARTITION BY FUNDSMOVE_INSTRUCTION_UID ORDER BY timestamp DESC) = 1
)
SELECT
  FI.CUSTOMER_UID,
  BI.ACCOUNT_UID,
  FI.AMOUNT
FROM
  FUNDSMOVE_INSTRUCTION FI
INNER JOIN
  pcb-{env}-landing.domain_payments.FUNDSMOVE_TYPE FT
ON
  FT.fundsmove_type_uid = FI.fundsmove_type_uid
INNER JOIN
  pcb-{env}-landing.domain_payments.FUNDSMOVE_SCHEDULE FS
ON
  FS.fundsmove_schedule_uid = FI.fundsmove_schedule_uid
INNER JOIN
  pcb-{env}-landing.domain_payments.BILLPAY_INSTRUCTION BI
ON
  BI.fundsmove_instruction_uid = FI.fundsmove_instruction_uid
WHERE
  FT.type_id = 'BTFR'
  AND FS.first_occurs_on < '2024-12-01'
  AND FS.first_occurs_on BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)
  AND CURRENT_DATE
UNION ALL
SELECT
  CAST(
    CASE
      WHEN ctti.paymentTypeInformation.categoryPurpose.proprietary IN ('INAD', 'INAR', 'INCA', 'INRC', 'INRR') THEN ( SELECT identification FROM UNNEST(FI_EVENT.fiToFiCustomerCreditTransfer.creditTransferTransactionInformation[SAFE_OFFSET(0)].creditor.identification.privateIdentification.other) WHERE schemeName.code='CUST')
      WHEN ctti.paymentTypeInformation.categoryPurpose.proprietary IN ('INSE',
      'INMR') THEN (
    SELECT
      identification
    FROM
      UNNEST(FI_EVENT.fiToFiCustomerCreditTransfer.creditTransferTransactionInformation[SAFE_OFFSET(0)].debtor.identification.privateIdentification.other)
    WHERE
      schemeName.code='CUST')
      ELSE NULL
  END
    AS INT) AS CUSTOMER_UID,
  CAST(CASE
      WHEN ctti.paymentTypeInformation.categoryPurpose.proprietary IN ('INAD', 'INAR', 'INCA', 'INRC', 'INRR') THEN ( SELECT identification FROM UNNEST(FI_EVENT.fiToFiCustomerCreditTransfer.creditTransferTransactionInformation[SAFE_OFFSET(0)].creditor.identification.privateIdentification.other) WHERE schemeName.proprietary='accountId')
      WHEN ctti.paymentTypeInformation.categoryPurpose.proprietary IN ('INSE',
      'INMR') THEN (
    SELECT
      identification
    FROM
      UNNEST(FI_EVENT.fiToFiCustomerCreditTransfer.creditTransferTransactionInformation[SAFE_OFFSET(0)].debtor.identification.privateIdentification.other)
    WHERE
      schemeName.proprietary='accountId')
      ELSE NULL
  END
    AS INT) AS ACCOUNT_UID,
  ctti.interbankSettlementAmount.amount AS AMOUNT,
FROM
  `pcb-{env}-landing.domain_payments.PCB_FUNDS_MOVE_CREATED` AS FI_EVENT,
  UNNEST(FI_EVENT.fiToFiCustomerCreditTransfer.creditTransferTransactionInformation) AS ctti
WHERE
  PAYMENT_RAIL='BTFR'
  AND DATE(ctti.acceptanceDateTime) BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)
  AND CURRENT_DATE