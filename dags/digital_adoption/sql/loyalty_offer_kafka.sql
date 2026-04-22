EXPORT DATA
  OPTIONS ( uri = 'gs://pcb-{env}-staging-extract/digital-adoption/loyalty-offer-*.parquet',
    format = 'PARQUET',
    OVERWRITE = TRUE ) AS (
  WITH
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
    WHERE
      CONDITION.effectiveDate <= CURRENT_DATE()
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY TRANS_ACC.accountId ORDER BY TRANS_ACC.postingDate DESC, TRANS_ACC.eventDateTime DESC, CONDITION.effectiveDate DESC) = 1 )
  SELECT
    CAST(OFFER_ACC.offerId AS STRING) AS offerId,
    CAST(OFFER_ACC.offerType AS STRING) AS offerType,
    CAST(OFFER_ACC.offerPriority AS STRING) AS offerPriority,
    CAST(OFFER_ACC.pcmaAccountId AS STRING) AS pcmaAccountId,
    CAST(OFFER_ACC.savingsAccountId AS STRING) AS savingsAccountId,
    CAST(OFFER_ACC.customerId AS STRING) AS customerId,
    CAST(OFFER_ACC.savingsAccountInd AS STRING) AS savingsAccountInd,
    CAST(OFFER_ACC.directDepositInd AS STRING) AS directDepositInd,
    CAST(OFFER_ACC.marginRate AS STRING) AS marginRate,
    CAST(OFFER_ACC.offerStatus AS STRING) AS offerStatus,
    CAST(OFFER_ACC.offerEffectiveDt AS STRING) AS offerEffectiveDt,
    CAST(OFFER_ACC.offerFulfilledDt AS STRING) AS offerFulfilledDt,
    CAST(OFFER_ACC.enrolmentEndDt AS STRING) AS enrolmentEndDt,
    CAST(OFFER_ACC.offerBenefitEndDt AS STRING) AS offerBenefitEndDt,
    CAST(OFFER_ACC.offerTermsViolatedDt AS STRING) AS offerTermsViolatedDt,
    CAST(OFFER_ACC.offerDisqualifiedDt AS STRING) AS offerDisqualifiedDt,
    CAST(COALESCE(BASE_RATE.effectiveRate, 0) - COALESCE(BASE_RATE.marginRate, 0) AS STRING) AS baseRate
  FROM
    `pcb-{env}-landing.domain_account_management.LOYALTY_OFFER_ACCOUNT` OFFER_ACC
  LEFT JOIN
    BASE_RATE
  ON
    OFFER_ACC.savingsAccountId = BASE_RATE.accountId
  WHERE
    DATE(OFFER_ACC.ingestion_timestamp) = CURRENT_DATE()
    AND OFFER_ACC.metadata.stream_name IS NULL);