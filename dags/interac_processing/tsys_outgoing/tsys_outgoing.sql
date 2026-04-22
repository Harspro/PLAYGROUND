WITH
  holidays AS (
  SELECT
    holiday_date,
    MIN(d) AS next_business_date
  FROM (
    SELECT
      h.holiday_date,
      d
    FROM
      `pcb-{env}-landing.domain_technical.VENDOR_HOLIDAY_DATES` h,
      UNNEST(GENERATE_DATE_ARRAY(h.holiday_date, DATE_ADD(h.holiday_date, INTERVAL 10 DAY), INTERVAL 1 DAY)) d
    WHERE
      EXTRACT(DAYOFWEEK
      FROM
        d) NOT IN (1,
        7)  -- Exclude weekends (Sunday=1, Saturday=7)
      AND d NOT IN (
      SELECT
        holiday_date
      FROM
        `pcb-{env}-landing.domain_technical.VENDOR_HOLIDAY_DATES`) )
  GROUP BY
    holiday_date)
SELECT * FROM (SELECT
  * EXCEPT (TRANSACTION_DATE),
  CASE
    WHEN DATE(TRANSACTION_DATE) IN ( SELECT holiday_date FROM holidays) THEN ( SELECT DATETIME(next_business_date) FROM holidays WHERE holiday_date = DATE(TRANSACTION_DATE))
    ELSE TRANSACTION_DATE
END
  AS REPORT_DATE
FROM (
  SELECT
    DISTINCT FI_A.MAST_ACCOUNT_ID AS ACCOUNT_NO,
    FI_AM.CARD_NUMBER AS CUSTOMER_CARD_NUMBER,
    ctti.interbankSettlementAmount.amount AS AMOUNT,
    ctti.debtor.name AS ACCOUNT_HOLDER_NAME,
    ctti.paymentIdentification.clearingSystemReference AS TRANSFER_REFERENCE_NUMBER,
    CASE
      WHEN TRANSFER_STATUS IS NULL THEN 'PENDING'
      WHEN TRANSFER_STATUS IN (10,
      11) THEN 'ACCEPTED'
      WHEN TRANSFER_STATUS IN (8) THEN 'CANCELLED'
  END
    AS INTERAC_STATUS,
    ctti.debtorAccount.identification.other.identification AS BANK_ACCOUNT_NUMBER,
    DATETIME(ctti.acceptanceDateTime, "America/Toronto") AS TRANSACTION_DATE_TIME,
    CASE EXTRACT(DAYOFWEEK
    FROM
      F.CREATE_DT)
      WHEN 1 THEN DATETIME_ADD(DATE(F.CREATE_DT), INTERVAL 1 DAY)
      WHEN 7 THEN DATETIME_ADD(DATE(F.CREATE_DT), INTERVAL 2 DAY)
    ELSE
    DATE(F.CREATE_DT)
  END
    AS TRANSACTION_DATE,
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
  WHERE
    FS.STATUS = 'TRANSMITTED'
    AND ENDS_WITH(FS.CREATE_FUNCTION_NAME, '-pcb-tsys-INTERAC-Req')
    AND FI_EVENT.payment_rail IN ('INSE',
      'INMR',
      'INCA',
      'INAR')))
WHERE
  REPORT_DATE = (CASE EXTRACT(DAYOFWEEK
    FROM
      CURRENT_DATE)
      WHEN 2 THEN DATETIME_SUB(DATE(CURRENT_DATE), INTERVAL 3 DAY) -- On Monday we generate Fridays report
    ELSE
    DATETIME_SUB(DATE(CURRENT_DATE), INTERVAL 1 DAY)
  END
    )