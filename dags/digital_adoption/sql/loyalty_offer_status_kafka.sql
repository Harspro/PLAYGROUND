EXPORT DATA
  OPTIONS ( uri = 'gs://pcb-{env}-staging-extract/digital-adoption/loyalty-offer-status-*.parquet',
    format = 'PARQUET',
    OVERWRITE = TRUE ) AS (
  WITH
    EXISTING_ACC AS(
    SELECT
      *
    FROM
      pcb-{env}-landing.domain_account_management.LOYALTY_OFFER_ACCOUNT
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY pcmaAccountId ORDER BY ingestion_timestamp DESC) =1
      AND UPPER(offerStatus) IN ('FULFILLED')),
    FULFILLED_EXPIRED_ACC AS(
    SELECT
      CAST(PENDING_ACC.offerId AS STRING) AS offerId,
      CAST(pcmaAccountId AS STRING) AS accountId,
      CAST(savingsAccountId AS STRING) AS savingsAccountId,
      CAST(customerId AS STRING) AS customerId,
      newStatus AS offerStatus,
      CAST(COALESCE(offerEffectiveDt,CURRENT_DATE()) AS STRING) AS offerEffectiveDt,
      CASE
        WHEN (newStatus ='EXPIRED') THEN CAST(0 AS STRING)
        ELSE CAST(REF.marginRate AS STRING)
    END
      AS marginRate
    FROM (
      SELECT
        *,
        CASE
          WHEN (savingsAccountInd IS TRUE AND directDepositInd IS TRUE AND enrolmentEndDt >= CURRENT_DATE()) THEN 'FULFILLED'
          ELSE CASE
          WHEN (enrolmentEndDt < CURRENT_DATE()) THEN 'EXPIRED'
      END
      END AS newStatus,
      FROM
        `pcb-{env}-landing.domain_account_management.LOYALTY_OFFER_ACCOUNT`
      QUALIFY
        ROW_NUMBER() OVER (PARTITION BY pcmaAccountId ORDER BY ingestion_timestamp DESC) =1
        AND UPPER(offerStatus) = 'PENDING') PENDING_ACC
    LEFT JOIN (
      SELECT
        *
      FROM
        `pcb-{env}-landing.domain_account_management.REF_LOYALTY_OFFER_DETAIL`
      QUALIFY
        ROW_NUMBER() OVER (PARTITION BY offerId ORDER BY ingestion_timestamp DESC) = 1
        AND offerActiveInd IS TRUE
        AND DATE_ADD(offerEndDate, INTERVAL enrolmentWindow + benefitPeriod DAY) >= CURRENT_DATE()) REF
    ON
      PENDING_ACC.offerId = REF.offerID
    WHERE
      newStatus IN ('FULFILLED',
        'EXPIRED')),
    DISQUALIFIED_ACC AS(
    SELECT
      CAST(offerId AS STRING) AS offerId,
      CAST(pcmaAccountId AS STRING) AS accountId,
      CAST(savingsAccountId AS STRING) AS savingsAccountId,
      CAST(customerId AS STRING) AS customerId,
      newStatus AS offerStatus,
      CAST(offerEffectiveDt AS STRING) AS offerEffectiveDt,
      CAST(0 AS STRING) AS marginRate
    FROM (
      SELECT
        *,
        CASE
          WHEN (savingsAccountInd IS FALSE OR DATE_DIFF(CURRENT_DATE(),COALESCE(offerTermsViolatedDt,'9999-12-31'),DAY)>30) THEN 'DISQUALIFIED'
          ELSE offerStatus
      END
        AS newStatus,
      FROM
        EXISTING_ACC
      WHERE
        offerBenefitEndDt >= CURRENT_DATE())
    WHERE
      UPPER(newStatus)='DISQUALIFIED'),
    COMPLETED_ACC AS (
    SELECT
      CAST(offerId AS STRING) AS offerId,
      CAST(pcmaAccountId AS STRING) AS accountId,
      CAST(savingsAccountId AS STRING) AS savingsAccountId,
      CAST(customerId AS STRING) AS customerId,
      'COMPLETED' AS offerStatus,
      CAST(offerEffectiveDt AS string) AS offerEffectiveDt,
      CAST(0 AS string) AS marginRate
    FROM
      EXISTING_ACC
    WHERE
      offerBenefitEndDt <= CURRENT_DATE()),
    BASE_RATE AS(
    SELECT
      TRANS_ACC.accountId AS accountId,
      CAST(COALESCE(TIER.effectiveRate, 0) - COALESCE(MARGINS.marginRate, 0) AS STRING) AS baseRate
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
    UNIONED_DATA AS (
    SELECT
      *,
      3 AS priority
    FROM
      DISQUALIFIED_ACC
    UNION ALL
    SELECT
      *,
      2 AS priority
    FROM
      FULFILLED_EXPIRED_ACC
    UNION ALL
    SELECT
      *,
      1 AS priority
    FROM
      COMPLETED_ACC)
  SELECT
    OFR_STATUS.*,
    CAST(BASE_RATE.baseRate AS STRING) AS baseRate
  FROM (
    SELECT
      * EXCEPT(priority)
    FROM
      UNIONED_DATA
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY accountId ORDER BY priority) = 1 ) OFR_STATUS
  LEFT JOIN
    BASE_RATE
  ON
    CAST(OFR_STATUS.savingsAccountId AS INT)=BASE_RATE.accountId);