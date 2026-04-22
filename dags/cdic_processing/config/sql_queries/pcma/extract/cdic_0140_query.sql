EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0140_latest_load AS (
  SELECT
    MAX( pcma_cdic_0140.REC_LOAD_TIMESTAMP ) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0140` AS pcma_cdic_0140 )
SELECT
  REPLACE(TO_HEX(SHA256(pcma_cdic_0140.ACCOUNT_UNIQUE_ID)), '|', '') AS Account_Unique_ID,
  pcma_cdic_0140.MI_DEPOSIT_HOLD_CODE AS MI_Deposit_Hold_Code,
  pcma_cdic_0140.MI_DEPOSIT_HOLD_SCHEDULED_RELEASE_DATE AS MI_Deposit_Hold_Scheduled_Release_Date,
  pcma_cdic_0140.CURRENCY_CODE AS Currency_Code,
  pcma_cdic_0140.MI_DEPOSIT_HOLD_AMOUNT AS MI_Deposit_Hold_Amount
FROM
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0140` AS pcma_cdic_0140
INNER JOIN
  pcma_cdic_0140_latest_load AS pcma_cdic_0140_ll
ON
  pcma_cdic_0140.REC_LOAD_TIMESTAMP = pcma_cdic_0140_ll.LATEST_REC_LOAD_TIMESTAMP;
