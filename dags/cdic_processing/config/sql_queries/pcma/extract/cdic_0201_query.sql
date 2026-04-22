EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0201_latest_load AS (
  SELECT
    MAX( pcma_cdic_0201.REC_LOAD_TIMESTAMP ) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0201` AS pcma_cdic_0201 )
SELECT
  pcma_cdic_0201.DEPOSITOR_TYPE_CODE AS Depositor_Type_Code,
  REPLACE(pcma_cdic_0201.MI_DEPOSITOR_TYPE, '|', '') AS MI_Depositor_Type,
  REPLACE(pcma_cdic_0201.DESCRIPTION, '|', '') AS Description
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0201` AS pcma_cdic_0201
INNER JOIN
  pcma_cdic_0201_latest_load AS pcma_cdic_0201_ll
ON
  pcma_cdic_0201.REC_LOAD_TIMESTAMP = pcma_cdic_0201_ll.LATEST_REC_LOAD_TIMESTAMP;