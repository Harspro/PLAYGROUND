EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0238_latest_load AS (
  SELECT
    MAX(pcma_cdic_0238.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0238` AS pcma_cdic_0238 )
SELECT
  pcma_cdic_0238.CLEARING_ACCOUNT_CODE AS Clearing_Account_Code,
  pcma_cdic_0238.MI_CLEARING_ACCOUNT AS MI_Clearing_Account,
  REPLACE(pcma_cdic_0238.DESCRIPTION, '|', '') AS Description
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0238` AS pcma_cdic_0238
INNER JOIN
  pcma_cdic_0238_latest_load AS pcma_cdic_0238_ll
ON
  pcma_cdic_0238.REC_LOAD_TIMESTAMP = pcma_cdic_0238_ll.LATEST_REC_LOAD_TIMESTAMP;