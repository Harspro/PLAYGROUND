EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0212_latest_load AS (
  SELECT
    MAX(pcma_cdic_0212.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0212` AS pcma_cdic_0212 )
SELECT
  pcma_cdic_0212.CDIC_PERSONAL_ID_TYPE_CODE AS CDIC_Personal_ID_Type_Code,
  REPLACE(pcma_cdic_0212.DESCRIPTION, '|', '') AS Description
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0212` AS pcma_cdic_0212
INNER JOIN
  pcma_cdic_0212_latest_load AS pcma_cdic_0212_ll
ON
  pcma_cdic_0212.REC_LOAD_TIMESTAMP = pcma_cdic_0212_ll.LATEST_REC_LOAD_TIMESTAMP;
