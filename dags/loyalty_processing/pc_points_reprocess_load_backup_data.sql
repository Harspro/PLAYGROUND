SELECT
pcPoints,
pcoErrorCode,
failureReasonCode,
transactionDatePartition,
pcfCustomerIdCluster,
METADATA,
INGESTION_TIMESTAMP
FROM `pcb-{env}-processing.domain_loyalty.PC_POINTS_REPROCESS_Back_Up`;