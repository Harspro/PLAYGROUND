SELECT
  COUNT(1) AS record_count
FROM (
  SELECT
    *
  FROM
    `pcb-{env}-landing.domain_account_management.REF_LOYALTY_OFFER_DETAIL`
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY offerId ORDER BY ingestion_timestamp DESC) = 1)
WHERE
  offerActiveInd IS TRUE
  AND offerStartDate <= CURRENT_DATE()
  AND DATE_ADD(offerEndDate, INTERVAL enrolmentWindow + benefitPeriod DAY) >= CURRENT_DATE();
