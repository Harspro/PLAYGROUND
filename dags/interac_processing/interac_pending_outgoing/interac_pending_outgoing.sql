SELECT
  *
FROM (
  WITH query as (
         SELECT
         CASE EXTRACT(DAYOFWEEK FROM CURRENT_DATE)
             -- On Monday generate Friday's report
             WHEN 2 THEN DATE(DATETIME_SUB(DATE(CURRENT_DATE), INTERVAL 3 DAY))
             ELSE DATE(DATETIME_SUB(DATE(CURRENT_DATE), INTERVAL 1 DAY))
         END AS FILTER_DATE
    ),
    refId_count AS (
    SELECT
      ctti.paymentIdentification.clearingSystemReference ref,
      COUNT(F.FUNDSMOVE_GL_REQ_UID) fm_count
    FROM
      `pcb-{env}-curated.domain_payments.FUNDSMOVE_GL_REQ` F
    INNER JOIN
      `pcb-{env}-curated.domain_payments.PCB_FUNDS_MOVE_CREATED` FI_EVENT
    ON
      F.FUNDSMOVE_INSTRUCTION_ID = CAST( (
        SELECT
          envelope.any
        FROM
          UNNEST(FI_EVENT.fiToFiCustomerCreditTransfer.supplementaryData)
        WHERE
          placeAndName='InstructionData.instructionId') AS INT),
      UNNEST(FI_EVENT.fiToFiCustomerCreditTransfer.creditTransferTransactionInformation) AS ctti, query
      WHERE DATE(F.CREATE_DT) <= FILTER_DATE
    GROUP BY
      ctti.paymentIdentification.clearingSystemReference )
  SELECT
    DISTINCT FI_A.MAST_ACCOUNT_ID AS ACCOUNT_NO,
    FI_AM.CARD_NUMBER AS CUSTOMER_CARD_NUMBER,
    ctti.interbankSettlementAmount.amount AS AMOUNT,
    ctti.debtor.name AS ACCOUNT_HOLDER_NAME,
    ctti.paymentIdentification.clearingSystemReference AS TRANSFER_REFERENCE_NUMBER,
    ctti.paymentTypeInformation.categoryPurpose.proprietary AS FUNDSMOVE_TYPE,
    DATETIME(fiToFiCustomerCreditTransfer.groupHeader.creationDateTime, "America/Toronto") AS CREATE_DT,
    CASE EXTRACT(DAYOFWEEK
    FROM
      F.CREATE_DT)
      WHEN 1 THEN DATETIME_ADD(DATE(F.CREATE_DT), INTERVAL 1 DAY)
      WHEN 7 THEN DATETIME_ADD(DATE(F.CREATE_DT), INTERVAL 2 DAY)
    ELSE
    DATE(F.CREATE_DT)
  END
    AS TSYS_DATE,
    ctti.debtorAccount.identification.other.identification AS BANK_ACCOUNT_NUMBER,
    CURRENT_DATETIME AS REPORT_DATE
  FROM
    `pcb-{env}-curated.domain_payments.FUNDSMOVE_GL_REQ` F
  LEFT JOIN
    `pcb-{env}-curated.domain_payments.FUNDSMOVE_GL_REQ_STATUS` FS
  ON
    F.FUNDSMOVE_GL_REQ_UID = FS.FUNDSMOVE_GL_REQ_UID
  LEFT JOIN
    `pcb-{env}-curated.domain_payments.PCB_FUNDS_MOVE_CREATED` FI_EVENT
  ON
    F.FUNDSMOVE_INSTRUCTION_ID = CAST( (
      SELECT
        envelope.any
      FROM
        UNNEST(FI_EVENT.fiToFiCustomerCreditTransfer.supplementaryData)
      WHERE
        placeAndName='InstructionData.instructionId') AS INT)
  LEFT JOIN
    `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` AS FI_AC
  ON
    FI_AC.ACCOUNT_UID= CAST( (
      SELECT
        identification
      FROM
        UNNEST(FI_EVENT.fiToFiCustomerCreditTransfer.creditTransferTransactionInformation[SAFE_OFFSET(0)].debtor.identification.privateIdentification.other)
      WHERE
        schemeName.proprietary='accountId') AS INT)
    AND FI_AC.CUSTOMER_UID=CAST( (
      SELECT
        identification
      FROM
        UNNEST(FI_EVENT.fiToFiCustomerCreditTransfer.creditTransferTransactionInformation[SAFE_OFFSET(0)].debtor.identification.privateIdentification.other)
      WHERE
        schemeName.code='CUST') AS INT)
    AND ACTIVE_IND = 'Y'
  LEFT JOIN
    `pcb-{env}-curated.domain_account_management.ACCOUNT` AS FI_A
  ON
    FI_AC.ACCOUNT_UID=FI_A.ACCOUNT_UID
  LEFT JOIN
    `pcb-{env}-curated.domain_account_management.ACCESS_MEDIUM` FI_AM
  ON
    FI_AC.ACCOUNT_CUSTOMER_UID = FI_AM.ACCOUNT_CUSTOMER_UID
    AND FI_AM.DEACTIVATED_DT IS NULL
    AND FI_AM.ACTIVATION_DT IS NOT NULL,
    UNNEST(FI_EVENT.fiToFiCustomerCreditTransfer.creditTransferTransactionInformation) AS ctti
  LEFT JOIN
    `pcb-{env}-curated.domain_payments.INTERAC_SETTLE_PRCSSD_XFER` ISP
  ON
    ctti.paymentIdentification.clearingSystemReference = ISP.TRANSFER_REFERENCE_NUMBER
  LEFT JOIN
    refId_count
  ON
    refId_count.ref = ctti.paymentIdentification.clearingSystemReference,
    query
  WHERE
    DATE(F.CREATE_DT) <= FILTER_DATE
    AND FS.STATUS = 'TRANSMITTED'
    AND ENDS_WITH(FS.CREATE_FUNCTION_NAME, '-pcb-tsys-INTERAC-Req')
    AND FI_EVENT.payment_rail IN ('INSE',
      'INMR')
    AND (TRANSFER_STATUS IS NULL
    -- if cancelled and only once entry in FUNDSMOVE_GL_REQ
      OR (DATE(OP_TRANSACTION_DT) > FILTER_DATE)
      OR ( TRANSFER_STATUS = 8 and
        DATE(OP_TRANSACTION_DT) = FILTER_DATE
        AND fm_count = 1) ))