CREATE OR REPLACE TABLE
  pcb-{env}-landing.domain_account_management.PC_DOWNLOAD_BACKUP
PARTITION BY
  FILE_CREATE_DT
CLUSTER BY
  ACCOUNT_NUMBER,
  ACCOUNT_IDENTIFIER
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
  pcb-{env}-landing.domain_account_management.PC_DOWNLOAD
WHERE
  FILE_CREATE_DT < '2025-09-17';       -- file_ingestion started from this date
