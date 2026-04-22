INSERT INTO
  pcb-{env}-landing.domain_account_management.REF_LOYALTY_OFFER_DETAIL
    (offerId,
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
  (1,'LOYALTY_RATE_INCREASE','Loyalty Offer PIV',30,TRUE,0.25,'2025-10-06','2025-10-10',31,10,1500,0,0,'PIV', DATETIME_TRUNC(CURRENT_DATETIME('America/Toronto'),SECOND), TIMESTAMP(DATETIME(CURRENT_TIMESTAMP(), "America/Toronto")));