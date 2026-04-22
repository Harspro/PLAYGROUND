EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0211_latest_load AS (
  SELECT
    MAX(pcma_cdic_0211.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0211` AS pcma_cdic_0211 )
SELECT
  pcma_cdic_0211.PERSONAL_ID_TYPE_CODE AS Personal_ID_Type_Code,
  REPLACE(pcma_cdic_0211.MI_PERSONAL_ID_TYPE, '|', '') AS MI_Personal_ID_Type,
  REPLACE(pcma_cdic_0211.DESCRIPTION, '|', '') AS Description,
  pcma_cdic_0211.CDIC_PERSONAL_ID_TYPE_CODE AS CDIC_Personal_ID_Type_Code
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0211` AS pcma_cdic_0211
INNER JOIN
  pcma_cdic_0211_latest_load AS pcma_cdic_0211_ll
ON
  pcma_cdic_0211.REC_LOAD_TIMESTAMP = pcma_cdic_0211_ll.LATEST_REC_LOAD_TIMESTAMP;