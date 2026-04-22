EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0239_latest_load AS (
  SELECT
    MAX( pcma_cdic_0239.REC_LOAD_TIMESTAMP ) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0239` AS pcma_cdic_0239 )
SELECT
  pcma_cdic_0239.ACCOUNT_TYPE_CODE AS Account_Type_Code,
  REPLACE(pcma_cdic_0239.MI_ACCOUNT_TYPE, '|', '') AS MI_Account_Type,
  REPLACE(pcma_cdic_0239.DESCRIPTION, '|', '') AS Description
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0239` AS pcma_cdic_0239
INNER JOIN
  pcma_cdic_0239_latest_load AS pcma_cdic_0239_ll
ON
  pcma_cdic_0239.REC_LOAD_TIMESTAMP = pcma_cdic_0239_ll.LATEST_REC_LOAD_TIMESTAMP;
