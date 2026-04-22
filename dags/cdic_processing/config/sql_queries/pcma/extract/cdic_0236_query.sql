EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0236_latest_load AS (
  SELECT
    MAX( pcma_cdic_0236.REC_LOAD_TIMESTAMP ) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0236` AS pcma_cdic_0236 )
SELECT
  pcma_cdic_0236.ACCOUNT_STATUS_CODE AS Account_Status_Code,
  REPLACE(pcma_cdic_0236.MI_ACCOUNT_STATUS_CODE, '|', '') AS MI_Account_Status_Code,
  REPLACE(pcma_cdic_0236.DESCRIPTION, '|', '') AS Description
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0236` AS pcma_cdic_0236
INNER JOIN
  pcma_cdic_0236_latest_load AS pcma_cdic_0236_ll
ON
  pcma_cdic_0236.REC_LOAD_TIMESTAMP = pcma_cdic_0236_ll.LATEST_REC_LOAD_TIMESTAMP;