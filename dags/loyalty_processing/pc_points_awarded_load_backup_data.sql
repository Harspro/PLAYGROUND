SELECT
pcPoints,
memberId,
pointEventId,
pointEventReference,
transactionDatePartition,
pcfCustomerIdCluster,
METADATA,
INGESTION_TIMESTAMP
FROM `pcb-{env}-processing.domain_loyalty.PC_POINTS_AWARDED_Back_Up`;