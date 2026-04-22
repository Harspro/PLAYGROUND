WITH PRODUCT_TRANSACTION AS (
  SELECT
    *
  FROM
    pcb-{env}-landing.domain_account_management.PRODUCT_TRANSACTION
  QUALIFY ROW_NUMBER() OVER (PARTITION BY referenceNumber, debitCreditInd ORDER BY INGESTION_TIMESTAMP DESC) = 1
)
SELECT
  customerId AS CUSTOMER_UID,
  accountId AS ACCOUNT_UID,
  sourceAccountId AS SOURCE_ACCOUNT_ID,
  interchangePercentageRate AS INTERCHANGE_PERCENTAGE_RATE,
  cardNumber AS CARD_NUMBER,
  PT.type AS TYPE,
  authorizedDt AS AUTHORIZED_DATE,
  postedDt AS POSTED_DATE,
  transactedDt AS TRANSACTED_DATE,
  DATETIME(TIMESTAMP(transactedTimestamp), 'America/Toronto') as TRANSACTED_TIMESTAMP,
  PT.description AS DESCRIPTION,
  fundsMoveType AS FUNDS_MOVE_TYPE_ID,
  instructionId AS INSTRUCTION_ID,
  pcbReferenceNumber AS PCB_REFERENCE_NUMBER,
  pcbTransactionNumber AS PCB_TRANSACTION_NO,
  referenceNumber AS REFERENCE_NUMBER,
  banknetReferenceNumber AS BANKNET_REFERENCE_NUMBER,
  authorizationCode AS AUTHORIZATION_CODE,
  transactionCategory AS TRANSACTION_CATEGORY,
  transactionCode AS TRANSACTION_CODE,
  finalAmount AS FINAL_AMOUNT,
  finalCurrencyCode AS FINAL_CURRENCY_CODE,
  originalAmount AS ORIGINAL_AMOUNT,
  originalCurrencyCode AS ORIGINAL_CURRENCY_CODE,
  foreignExchangeRate AS FOREIGN_EXCHANGE_RATE,
  foreignExchangeFee AS FOREIGN_EXCHANGE_FEE,
  DATE(billingCycleDt) AS BILLING_CYCLE_DATE,
  debitCreditInd AS DEBIT_CREDIT_IND,
  merchantId AS MERCHANT_ID,
  merchantName AS MERCHANT_NAME,
  merchantCity AS MERCHANT_CITY,
  merchantPostalZipCode AS MERCHANT_POSTAL_ZIP_CODE,
  merchantProvinceState AS MERCHANT_PROVINCE_STATE,
  merchantCountry AS MERCHANT_COUNTRY,
  posEntryMode AS POS_ENTRY_MODE,
  settlementDt AS SETTLEMENT_DT,
  deviceType AS DEVICE_TYPE,
  loyaltyPointsCalcInd AS LOYALTY_POINTS_CALC_IND,
  disputableInd AS DISPUTABLE_IND,
  onlineInd AS ONLINE_IND,
  disputeRequestInd AS DISPUTE_REQUEST_IND,
  recurringInd AS RECURRING_IND,
  disputeInd AS DISPUTE_IND,
  fraudInd AS FRAUD_IND,
  memoInd AS MEMO_IND,
  forcePostInd AS FORCE_POST_IND,
  transferInd AS TRANSFER_IND,
  translateInd AS TRANSLATE_IND,
  chgbkInd AS CHGBK_IND,
  adjustInd AS ADJUST_IND,
  retrieveRequestInd AS RETRIEVE_REQUEST_IND,
  nonPrintableInd AS NON_PRINTABLE_IND,
  reversalInd AS REVERSAL_IND,
  centralProcDt AS CENTRAL_PROC_DT,
  AM.ACCESS_MEDIUM_NO AS ORIGINAL_CARD_NUMBER,
  AC.ACCOUNT_CUSTOMER_NO AS SOURCE_CUSTOMER_ID,
  CI.CUSTOMER_IDENTIFIER_NO AS PCF_CUST_ID,
  P.CODE AS PRODUCT_CODE,
  P.TYPE AS PRODUCT_TYPE,
  RMC.CODE AS MERCHANT_CATEGORY_CODE,
  DATETIME(TIMESTAMP(INGESTION_TIMESTAMP), 'America/Toronto') AS RECORD_LOAD_TIMESTAMP,
FROM
  PRODUCT_TRANSACTION PT
  LEFT JOIN pcb-{env}-landing.domain_account_management.ACCESS_MEDIUM AM on PT.originalCardId=AM.ACCESS_MEDIUM_UID
  LEFT JOIN pcb-{env}-landing.domain_account_management.ACCOUNT_CUSTOMER AC on PT.accountCustomerId=AC.ACCOUNT_CUSTOMER_UID
  LEFT JOIN pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER CI on PT.customerId=CI.CUSTOMER_UID and CI.TYPE = 'PCF-CUSTOMER-ID' and CI.DISABLED_IND = 'N'
  LEFT JOIN pcb-{env}-landing.domain_account_management.ACCOUNT A on PT.accountId=A.ACCOUNT_UID
  LEFT JOIN pcb-{env}-landing.domain_account_management.PRODUCT P on A.PRODUCT_UID=P.PRODUCT_UID
  LEFT JOIN pcb-{env}-landing.domain_account_management.REF_MERCHANT_CATEGORY RMC on PT.refMerchantCategoryId=RMC.REF_MERCHANT_CATEGORY_UID
