EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0999_latest_load AS (
  SELECT
    MAX(pcma_cdic_0999.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0999` AS pcma_cdic_0999 )
SELECT
  pcma_cdic_0999.SUBSYSTEM_ID AS Subsystem_ID,
  REPLACE(pcma_cdic_0999.MI_SUBSYSTEM_CODE, '|', '') AS MI_Subsystem_Code,
  REPLACE(pcma_cdic_0999.DESCRIPTION, '|', '') AS Description
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0999` AS pcma_cdic_0999
INNER JOIN
  pcma_cdic_0999_latest_load AS pcma_cdic_0999_ll
ON
  pcma_cdic_0999.REC_LOAD_TIMESTAMP = pcma_cdic_0999_ll.LATEST_REC_LOAD_TIMESTAMP;