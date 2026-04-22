EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0600_latest_load AS (
  SELECT
    MAX(pcma_cdic_0600.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0600` AS pcma_cdic_0600 )
SELECT
  REPLACE(pcma_cdic_0600.LEDGER_ACCOUNT, '|', '') AS Ledger_Account,
  REPLACE(pcma_cdic_0600.LEDGER_DESCRIPTION, '|', '') AS Ledger_Description,
  REPLACE(pcma_cdic_0600.LEDGER_FLAG, '|', '') AS Ledger_Flag,
  REPLACE(pcma_cdic_0600.GL_ACCOUNT, '|', '') AS GL_Account,
  pcma_cdic_0600.DEBIT AS Debit,
  pcma_cdic_0600.CREDIT AS Credit,
  REPLACE(pcma_cdic_0600.ACCOUNT_UNIQUE_ID, '|', '') AS Account_Unique_ID,
  REPLACE(pcma_cdic_0600.ACCOUNT_NUMBER, '|', '') AS Account_Number,
  pcma_cdic_0600.ACCOUNT_BALANCE AS Account_Balance
FROM
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0600` AS pcma_cdic_0600
INNER JOIN
  pcma_cdic_0600_latest_load AS pcma_cdic_0600_ll
ON
  pcma_cdic_0600.REC_LOAD_TIMESTAMP = pcma_cdic_0600_ll.LATEST_REC_LOAD_TIMESTAMP;
