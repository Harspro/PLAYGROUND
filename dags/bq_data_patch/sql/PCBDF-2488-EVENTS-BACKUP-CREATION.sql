CREATE OR REPLACE TABLE
  pcb-{env}-landing.domain_account_management.EVENTS_BACKUP
PARTITION BY
  DATE(EV3_EL01_JULIAN_DATE)
CLUSTER BY
  EV3_EL01_TYPE_EVENT,
  EV3_EVENT_FIELD_NUM,
  EV3_EL01_ACCOUNT_NUMBER,
  FILE_RECORD_NBR 
OPTIONS(
  expiration_timestamp = TIMESTAMP_ADD(
    TIMESTAMP(CURRENT_DATETIME('America/Toronto')), 
    INTERVAL 15 DAY
  )
)
AS
SELECT
  *
FROM
  pcb-{env}-landing.domain_account_management.EVENTS
WHERE
  FILE_CREATE_DT >= '2024-10-25';       -- file_ingestion started from this date
