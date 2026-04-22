WITH
  BASE_RATE AS(
  SELECT
    TRANS_ACC.accountId AS accountId,
    TRANS_ACC.postingDate AS postingDate,
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
    ROW_NUMBER() OVER (PARTITION BY TRANS_ACC.accountId ORDER BY TRANS_ACC.postingDate DESC, TRANS_ACC.eventDateTime DESC, CONDITION.effectiveDate DESC) = 1 ),
  PCMA_ACC AS(
  SELECT
    *
  FROM
    pcb-{env}-landing.domain_account_management.LOYALTY_OFFER_ELIGIBLE_ACCOUNT
  WHERE
    DATE(rec_load_timestamp)=CURRENT_DATE()),
  EXISTING_ACC AS (
  SELECT
    *
  FROM
    pcb-{env}-landing.domain_account_management.LOYALTY_OFFER_ACCOUNT
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY pcmaAccountId ORDER BY ingestion_timestamp DESC) =1
    AND UPPER(offerStatus) != ('EXPIRED')),
  OFFER_DETAIL AS(
  SELECT
    *
  FROM
    pcb-{env}-landing.domain_account_management.REF_LOYALTY_OFFER_DETAIL
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY offerId ORDER BY ingestion_timestamp DESC) = 1
    AND offerActiveInd IS TRUE
    AND DATE_ADD(offerEndDate, INTERVAL enrolmentWindow + benefitPeriod DAY) >= CURRENT_DATE()),
  DELTA_ACC AS(
  SELECT
    *
  FROM (
    SELECT
      PCMA_ACC.*,
      EXISTING_ACC.*,
    FROM
      PCMA_ACC
    LEFT JOIN
      EXISTING_ACC
    ON
      PCMA_ACC.pcma_account_id=EXISTING_ACC.pcmaAccountId)
  WHERE
    (offerStatus IS NULL AND DATE(rec_load_timestamp) <= (SELECT offerEndDate FROM OFFER_DETAIL))
    OR ((savings_account_ind != savingsAccountInd
        OR direct_deposit_ind != directDepositInd)
      AND UPPER(offerStatus) NOT IN ('DISQUALIFIED',
        'FAILED',
        'RESET-FAILED',
        'COMPLETED'))),
  NEW_ACC AS(
  SELECT
    *
  FROM (
    SELECT
      DELTA_ACC.*,
      COALESCE(BASE_RATE.marginRate,0) AS activeMarginRate
    FROM
      DELTA_ACC
    LEFT JOIN
      BASE_RATE
    ON
      DELTA_ACC.savings_account_id = BASE_RATE.accountId)
  WHERE
    (offerStatus='FULFILLED'
      OR activeMarginRate=0))
SELECT
  CASE WHEN(UPPER(NEW_ACC.offerStatus) = ('FULFILLED')) THEN NEW_ACC.offerId
    ELSE OFFER_DETAIL.offerId
END
  AS offerId,
  CASE WHEN(UPPER(NEW_ACC.offerStatus) = ('FULFILLED')) THEN NEW_ACC.offerType
    ELSE OFFER_DETAIL.offerType
END
  AS offerType,
  CASE WHEN(UPPER(NEW_ACC.offerStatus) = ('FULFILLED')) THEN NEW_ACC.offerPriority
    ELSE OFFER_DETAIL.offerPriority
END
  AS offerPriority,
  NEW_ACC.pcma_account_id AS pcmaAccountId,
  NEW_ACC.savings_account_id AS savingsAccountId,
  NEW_ACC.customer_id AS customerId,
  NEW_ACC.savings_account_ind AS savingsAccountInd,
  NEW_ACC.direct_deposit_ind AS directDepositInd,
  CASE WHEN(UPPER(NEW_ACC.offerStatus) = ('FULFILLED')) THEN NEW_ACC.marginRate
    ELSE OFFER_DETAIL.marginRate
END
  AS marginRate,
  CASE WHEN(UPPER(NEW_ACC.offerStatus) = ('FULFILLED')) THEN NEW_ACC.offerStatus
    ELSE 'PENDING'
END
  AS offerStatus,
  CASE WHEN(UPPER(NEW_ACC.offerStatus) = ('FULFILLED')) THEN NEW_ACC.offerEffectiveDt
    ELSE CASE WHEN(NEW_ACC.savings_account_ind IS TRUE AND NEW_ACC.direct_deposit_ind IS TRUE) THEN CURRENT_DATE()
    ELSE CAST(NULL AS DATE)
END
END AS offerEffectiveDt,
  CASE WHEN(UPPER(NEW_ACC.offerStatus) = ('FULFILLED')) THEN NEW_ACC.offerFulfilledDt
    ELSE CAST(NULL AS DATE)
END
  AS offerFulfilledDt,
  CASE WHEN(UPPER(NEW_ACC.offerStatus) = ('FULFILLED')) THEN NEW_ACC.enrolmentEndDt
    ELSE DATE_ADD(OFFER_DETAIL.offerEndDate, INTERVAL OFFER_DETAIL.enrolmentWindow DAY)
END
  AS enrolmentEndDt,
  CASE WHEN(UPPER(NEW_ACC.offerStatus) = ('FULFILLED')) THEN NEW_ACC.offerBenefitEndDt
    ELSE CAST(NULL AS DATE)
END
  AS offerBenefitEndDt,
  CASE WHEN(UPPER(NEW_ACC.offerStatus) = ('FULFILLED')) THEN CASE WHEN(NEW_ACC.direct_deposit_ind IS FALSE AND NEW_ACC.offerTermsViolatedDt IS NULL) THEN CURRENT_DATE()
    ELSE CASE WHEN(NEW_ACC.direct_deposit_ind IS TRUE) THEN CAST(NULL AS DATE)
    ELSE NEW_ACC.offerTermsViolatedDt
END
END
    ELSE CAST(NULL AS DATE)
END
  AS offerTermsViolatedDt,
  CASE WHEN(UPPER(NEW_ACC.offerStatus) = ('FULFILLED')) THEN NEW_ACC.offerDisqualifiedDt
    ELSE CAST(NULL AS DATE)
END
  AS offerDisqualifiedDt,
  STRUCT(CAST(NULL AS STRING) AS stream_name,
    NULL AS topic_partition,
    NULL AS partition_offset,
    NULL AS stream_offset,
    NULL AS offset_diff,
    CAST(NULL AS STRING) AS environment,
    NULL AS topic_version,
    CAST(NULL AS STRING) AS uservice_correlation_id,
    CAST(NULL AS STRING) AS uservice_traceability_id) AS METADATA,
  CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM
  NEW_ACC
CROSS JOIN
  OFFER_DETAIL;