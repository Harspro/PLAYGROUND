SELECT
  COUNT(1) AS record_count
FROM (
  SELECT
    pcmaAccountId
  FROM
    `pcb-{env}-landing.domain_account_management.LOYALTY_OFFER_ACCOUNT`
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY pcmaAccountId ORDER BY ingestion_timestamp DESC) =1
    AND UPPER(offerStatus) = 'PENDING'
    AND ((savingsAccountInd IS TRUE
        AND directDepositInd IS TRUE
        AND enrolmentEndDt >= CURRENT_DATE())
      OR (enrolmentEndDt < CURRENT_DATE()))
  UNION ALL
  SELECT
    pcmaAccountId
  FROM
    pcb-{env}-landing.domain_account_management.LOYALTY_OFFER_ACCOUNT
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY pcmaAccountId ORDER BY ingestion_timestamp DESC) =1
    AND UPPER(offerStatus) = 'FULFILLED'
    AND offerBenefitEndDt >= CURRENT_DATE()
    AND (savingsAccountInd IS FALSE
      OR DATE_DIFF(CURRENT_DATE(),COALESCE(offerTermsViolatedDt,'9999-12-31'),DAY)>30)
  UNION ALL
  SELECT
    pcmaAccountId
  FROM
    pcb-{env}-landing.domain_account_management.LOYALTY_OFFER_ACCOUNT
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY pcmaAccountId ORDER BY ingestion_timestamp DESC) =1
    AND offerStatus = 'FULFILLED'
    AND offerBenefitEndDt <= CURRENT_DATE());