EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0202_latest_load AS (
  SELECT
    MAX( pcma_cdic_0202.REC_LOAD_TIMESTAMP ) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0202` AS pcma_cdic_0202 )
SELECT
  pcma_cdic_0202.PHONE_TYPE_CODE AS Phone_Type_Code,
  REPLACE(pcma_cdic_0202.DESCRIPTION, '|', '') AS Description
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0202` AS pcma_cdic_0202
INNER JOIN
  pcma_cdic_0202_latest_load AS pcma_cdic_0202_ll
ON
  pcma_cdic_0202.REC_LOAD_TIMESTAMP = pcma_cdic_0202_ll.LATEST_REC_LOAD_TIMESTAMP;
