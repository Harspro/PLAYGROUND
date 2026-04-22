EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0235_latest_load AS (
  SELECT
    MAX(pcma_cdic_0235.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0235` AS pcma_cdic_0235 )
SELECT
  pcma_cdic_0235.CDIC_HOLD_STATUS_CODE AS CDIC_Hold_Status_Code,
  REPLACE(pcma_cdic_0235.CDIC_HOLD_STATUS, '|', '') AS CDIC_Hold_Status
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0235` AS pcma_cdic_0235
INNER JOIN
  pcma_cdic_0235_latest_load AS pcma_cdic_0235_ll
ON
  pcma_cdic_0235.REC_LOAD_TIMESTAMP = pcma_cdic_0235_ll.LATEST_REC_LOAD_TIMESTAMP;