EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0401_latest_load AS (
  SELECT
    MAX( pcma_cdic_0401.REC_LOAD_TIMESTAMP ) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0401` AS pcma_cdic_0401 )
SELECT
  pcma_cdic_0401.TRANSACTION_CODE AS Transaction_Code,
  REPLACE(pcma_cdic_0401.MI_TRANSACTION_CODE, '|', '') AS MI_Transaction_Code,
  REPLACE(pcma_cdic_0401.DESCRIPTION, '|', '') AS Description
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0401` AS pcma_cdic_0401
INNER JOIN
  pcma_cdic_0401_latest_load AS pcma_cdic_0401_ll
ON
  pcma_cdic_0401.REC_LOAD_TIMESTAMP = pcma_cdic_0401_ll.LATEST_REC_LOAD_TIMESTAMP;