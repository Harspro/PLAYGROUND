Select
      pcPoints.id,
      pcfCustomerIdCluster as pcfCustomerId,
      pcPoints.customerId,
      pcPoints.campaignId,
      pcPoints.externalCampaignId,
      pcPoints.points,
      DATETIME(transactionDatePartition, substr(pcPoints.transactionDate, -6, 6)) as transactionDate,
      pcPoints.description,
      pcPoints.externalId,
      pcPoints.transactionOrigin,
      pcPoints.cardHash,
      pcPoints.cardBin,
      pcPoints.transactionCode,
      pcoErrorCode,
      failureReasonCode,
      INGESTION_TIMESTAMP
    from pcb-{env}-landing.domain_loyalty.PC_POINTS_REPROCESS;
