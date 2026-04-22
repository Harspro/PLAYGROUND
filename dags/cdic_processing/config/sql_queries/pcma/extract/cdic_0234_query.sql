EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0234_latest_load AS (
  SELECT
    MAX( pcma_cdic_0234.REC_LOAD_TIMESTAMP ) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0234` AS pcma_cdic_0234 )
SELECT
  pcma_cdic_0234.INSURANCE_DETERMINATION_CATEGORY_TYPE_CODE AS Insurance_Determination_Category_Type_Code,
  REPLACE(pcma_cdic_0234.DESCRIPTION, '|', '') AS Description
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0234` AS pcma_cdic_0234
INNER JOIN
  pcma_cdic_0234_latest_load AS pcma_cdic_0234_ll
ON
  pcma_cdic_0234.REC_LOAD_TIMESTAMP = pcma_cdic_0234_ll.LATEST_REC_LOAD_TIMESTAMP;