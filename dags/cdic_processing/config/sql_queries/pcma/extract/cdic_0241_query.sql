EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0241_latest_load AS (
  SELECT
    MAX(pcma_cdic_0241.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0241` AS pcma_cdic_0241 )
SELECT
  pcma_cdic_0241.MI_DEPOSIT_HOLD_CODE AS MI_Deposit_Hold_Code,
  REPLACE(pcma_cdic_0241.MI_DEPOSIT_HOLD_TYPE, '|', '') AS MI_Deposit_Hold_Type,
  REPLACE(pcma_cdic_0241.DESCRIPTION, '|', '') AS Description
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0241` AS pcma_cdic_0241
INNER JOIN
  pcma_cdic_0241_latest_load AS pcma_cdic_0241_ll
ON
  pcma_cdic_0241.REC_LOAD_TIMESTAMP = pcma_cdic_0241_ll.LATEST_REC_LOAD_TIMESTAMP;