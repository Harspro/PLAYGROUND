UPDATE
  pcb-{env}-landing.domain_account_management.REF_LOYALTY_OFFER_DETAIL
SET
  offerActiveInd = FALSE
WHERE
  offerId = 2;

INSERT INTO
  pcb-{env}-landing.domain_account_management.REF_LOYALTY_OFFER_DETAIL (offerId,
    offerType,
    offerDescription,
    offerPriority,
    offerActiveInd,
    marginRate,
    offerStartDate,
    offerEndDate,
    enrolmentWindow,
    benefitPeriod,
    directDepositThreshold,
    averageInflowThreshold,
    inflowThresholdPeriod,
    offerCreatedBy,
    offerUpdatedDt,
    ingestion_timestamp)
VALUES
  (3,'LOYALTY_RATE_INCREASE','Loyalty Offer November 2025',30,TRUE,0.7,'2025-11-13','2026-02-11',30,365,1500,0,0,'MANUAL', DATETIME_TRUNC(CURRENT_DATETIME('America/Toronto'),SECOND), TIMESTAMP(DATETIME(CURRENT_TIMESTAMP(),"America/Toronto")));