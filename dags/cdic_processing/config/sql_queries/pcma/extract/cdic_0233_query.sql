EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0233_latest_load AS (
  SELECT
    MAX(pcma_cdic_0233.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0233` AS pcma_cdic_0233 )
SELECT
  pcma_cdic_0233.CURRENCY_CODE AS Currency_Code,
  REPLACE(pcma_cdic_0233.MI_CURRENCY_CODE, '|', '') AS MI_Currency_Code,
  REPLACE(pcma_cdic_0233.ISO_CURRENCY_CODE, '|', '') AS ISO_Currency_Code,
  REPLACE(pcma_cdic_0233.DESCRIPTION, '|', '') AS Description
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0233` AS pcma_cdic_0233
INNER JOIN
  pcma_cdic_0233_latest_load AS pcma_cdic_0233_ll
ON
  pcma_cdic_0233.REC_LOAD_TIMESTAMP = pcma_cdic_0233_ll.LATEST_REC_LOAD_TIMESTAMP;
