EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0240_latest_load AS (
  SELECT
    MAX( pcma_cdic_0240.REC_LOAD_TIMESTAMP ) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0240` AS pcma_cdic_0240 )
SELECT
  pcma_cdic_0240.CDIC_PRODUCT_GROUP_CODE AS CDIC_Product_Group_Code,
  REPLACE(pcma_cdic_0240.CDIC_PRODUCT_GROUP, '|', '') AS CDIC_Product_Group,
  REPLACE(pcma_cdic_0240.DESCRIPTION, '|', '') AS Description
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0240` AS pcma_cdic_0240
INNER JOIN
  pcma_cdic_0240_latest_load AS pcma_cdic_0240_ll
ON
  pcma_cdic_0240.REC_LOAD_TIMESTAMP = pcma_cdic_0240_ll.LATEST_REC_LOAD_TIMESTAMP;