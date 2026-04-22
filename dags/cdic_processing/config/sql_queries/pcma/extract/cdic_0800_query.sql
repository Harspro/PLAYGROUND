EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0800_latest_load AS (
  SELECT
    MAX( pcma_cdic_0800.REC_LOAD_TIMESTAMP ) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0800` AS pcma_cdic_0800 )
SELECT
  REPLACE(TO_HEX(SHA256(pcma_cdic_0800.ACCOUNT_UNIQUE_ID)), '|', '') AS Account_Unique_ID,
  pcma_cdic_0800.SUBSYSTEM_ID AS Subsystem_ID,
  pcma_cdic_0800.CDIC_HOLD_STATUS_CODE AS CDIC_Hold_Status_Code,
  pcma_cdic_0800.ACCOUNT_BALANCE AS Account_Balance,
  pcma_cdic_0800.ACCESSIBLE_BALANCE AS Accessible_Balance,
  pcma_cdic_0800.CDIC_HOLD_AMOUNT AS CDIC_Hold_Amount,
  pcma_cdic_0800.CURRENCY_CODE AS Currency_Code
FROM
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0800` AS pcma_cdic_0800
INNER JOIN
  pcma_cdic_0800_latest_load AS pcma_cdic_0800_ll
ON
  pcma_cdic_0800.REC_LOAD_TIMESTAMP = pcma_cdic_0800_ll.LATEST_REC_LOAD_TIMESTAMP;