EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0221_latest_load AS (
  SELECT
    MAX( pcma_cdic_0221.REC_LOAD_TIMESTAMP ) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0221` AS pcma_cdic_0221 )
SELECT
  pcma_cdic_0221.ADDRESS_TYPE_CODE AS Address_Type_Code,
  REPLACE(pcma_cdic_0221.MI_ADDRESS_TYPE, '|', '') AS MI_Address_Type,
  REPLACE(pcma_cdic_0221.DESCRIPTION, '|', '') AS Description
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0221` AS pcma_cdic_0221
INNER JOIN
  pcma_cdic_0221_latest_load AS pcma_cdic_0221_ll
ON
  pcma_cdic_0221.REC_LOAD_TIMESTAMP = pcma_cdic_0221_ll.LATEST_REC_LOAD_TIMESTAMP;