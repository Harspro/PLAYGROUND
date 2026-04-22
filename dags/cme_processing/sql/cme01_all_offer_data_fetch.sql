CREATE OR REPLACE TABLE pcb-{env}-processing.domain_marketing.CME_BATCH_STAGING
OPTIONS (
expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR)
)
AS
SELECT * FROM pcb-{env}-processing.domain_marketing.CME_BATCH_STAGE_CLI
UNION ALL
SELECT * FROM pcb-{env}-processing.domain_marketing.CME_BATCH_STAGE_AAU
UNION ALL
SELECT * FROM pcb-{env}-processing.domain_marketing.CME_BATCH_STAGE_PCH
;