EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0231_latest_load AS (
  SELECT
    MAX( pcma_cdic_0231.REC_LOAD_TIMESTAMP ) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0231` AS pcma_cdic_0231 )
SELECT
  pcma_cdic_0231.PRODUCT_CODE AS Product_Code,
  REPLACE(pcma_cdic_0231.MI_PRODUCT_CODE, '|', '') AS MI_Product_Code,
  REPLACE(pcma_cdic_0231.DESCRIPTION, '|', '') AS Description,
  pcma_cdic_0231.CDIC_PRODUCT_GROUP_CODE AS CDIC_Product_Group_Code
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0231` AS pcma_cdic_0231
INNER JOIN
  pcma_cdic_0231_latest_load AS pcma_cdic_0231_ll
ON
  pcma_cdic_0231.REC_LOAD_TIMESTAMP = pcma_cdic_0231_ll.LATEST_REC_LOAD_TIMESTAMP;