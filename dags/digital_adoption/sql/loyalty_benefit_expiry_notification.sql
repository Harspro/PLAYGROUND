EXPORT DATA OPTIONS (
  uri = (
    FORMAT(
      'gs://communications-inbound-pcb{deploy_env_storage_suffix}/loyalty_benefit_expiry/pcb_int_comms_loyalty_benefit_expiry_%s-*.csv',
      FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP())
    )
  ),
    format = 'CSV',
    field_delimiter = '|',
    OVERWRITE = TRUE,
    header = TRUE
  ) AS
WITH
  OFFER_REF AS(
  SELECT
    offerId,
    benefitPeriod,
    directDepositThreshold,
    offerEndDate
  FROM
    `pcb-{env}-landing.domain_account_management.REF_LOYALTY_OFFER_DETAIL`
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY offerId ORDER BY ingestion_timestamp DESC) = 1),
  NOTIF_ACC AS(
  SELECT
    CASE
      WHEN DATE_DIFF(offerBenefitEndDt,CURRENT_DATE(),DAY)=5 THEN 'LOYALTY-OFFER-PCMA-5DAYNOTICE'
      ELSE 'LOYALTY-OFFER-PCMA-21DAYNOTICE'
  END
    AS communicationCode,
    offerId,
    pcmaAccountId,
    savingsAccountId,
    customerId,
    offerFulfilledDt,
    marginRate,
    offerBenefitEndDt,
    enrolmentEndDt
  FROM
    `pcb-{env}-landing.domain_account_management.LOYALTY_OFFER_ACCOUNT`
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY pcmaAccountId ORDER BY ingestion_timestamp DESC) =1
    AND UPPER(offerStatus)='FULFILLED'
    AND DATE_DIFF(offerBenefitEndDt,CURRENT_DATE(),DAY) IN (5,
      21)),
  BASE_RATE AS(
  SELECT
    TRANS_ACC.accountId AS accountId,
    TIER.effectiveRate AS effectiveRate,
    COALESCE(MARGINS.marginRate,0) AS marginRate
  FROM
    `pcb-{env}-landing.domain_account_management.TRANSACT_ACCOUNT` TRANS_ACC
  LEFT JOIN
    UNNEST(TRANS_ACC.interests) AS INTEREST
  LEFT JOIN
    UNNEST(INTEREST.interestConditions) AS CONDITION
  LEFT JOIN
    UNNEST(CONDITION.tierDetails) AS TIER
  LEFT JOIN
    UNNEST(TIER.margins) AS MARGINS
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY TRANS_ACC.accountId ORDER BY TRANS_ACC.postingDate DESC, TRANS_ACC.eventDateTime DESC) = 1 )
SELECT
  NOTIF_ACC.communicationCode AS communicationCode,
  GENERATE_UUID() AS uniqueId,
  'LOYALTY-OFFER-PCMA' AS source,
  '' AS sourceIdempotencyKey,
  '' AS language,
  '' AS contactEmail,
  '' AS contactPhone,
  '' AS alternatePhone,
  '' AS pushToken,
  '' AS channels,
  '' AS domainCode,
  '' AS tenant,
  CURRENT_DATE('America/Toronto') AS createdDt,
  '' AS pcfCustomerId,
  pcmaAccountId AS accountId,
  customerId AS customerId,
  '' AS accountNumber,
  '' AS customerNumber,
  '' AS productCode,
  '' AS lastDigits,
  '' AS recipientTimezone,
  '' AS customerRole,
  TO_JSON_STRING([
  STRUCT(
    'BASE_RATE' AS paramName,
    CAST((COALESCE(BASE_RATE.effectiveRate, 0) - COALESCE(BASE_RATE.marginRate, 0)) AS STRING) AS paramValue,
    'NUMBER' AS paramType
  ),
  STRUCT(
    'MARGIN_RATE' AS paramName,
    CAST(NOTIF_ACC.marginRate AS STRING) AS paramValue,
    'NUMBER' AS paramType
  ),
  STRUCT(
    'TOTAL_RATE' AS paramName,
    CAST(COALESCE(BASE_RATE.effectiveRate, 0) - COALESCE(BASE_RATE.marginRate, 0) + NOTIF_ACC.marginRate AS STRING) AS paramValue,
    'NUMBER' AS paramType
  ),
  STRUCT(
    'ENROLMENT_END_DATE' AS paramName,
    FORMAT_DATE('%Y-%m-%d', NOTIF_ACC.enrolmentEndDt) AS paramValue,
    'DATE' AS paramType
  ),
  STRUCT(
    'BENEFIT_PERIOD' AS paramName,
    CAST(COALESCE(OFFER_REF.benefitPeriod, 0) AS STRING) AS paramValue,
    'NUMBER' AS paramType
  ),
  STRUCT(
    'DIRECT_DEPOSIT_THRESHOLD' AS paramName,
    CAST(COALESCE(OFFER_REF.directDepositThreshold, 0) AS STRING) AS paramValue,
    'CURRENCY' AS paramType
  ),
  STRUCT(
    'OFFER_END_DATE' AS paramName,
    FORMAT_DATE('%Y-%m-%d', OFFER_REF.offerEndDate) AS paramValue,
    'DATE' AS paramType
  ),
  STRUCT(
    'OFFER_BENEFIT_END_DATE' AS paramName,
    FORMAT_DATE('%Y-%m-%d', NOTIF_ACC.offerBenefitEndDt) AS paramValue,
    'DATE' AS paramType
  )]) AS parameters
FROM
  NOTIF_ACC
LEFT JOIN
  BASE_RATE
ON
  NOTIF_ACC.savingsAccountId = BASE_RATE.accountId
LEFT JOIN
  OFFER_REF
ON
  NOTIF_ACC.offerId = OFFER_REF.offerId;