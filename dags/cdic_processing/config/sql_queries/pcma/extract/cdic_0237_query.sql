EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0237_latest_load AS (
  SELECT
    MAX(pcma_cdic_0237.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0237` AS pcma_cdic_0237 )
SELECT
  pcma_cdic_0237.TRUST_ACCOUNT_TYPE_CODE AS Trust_Account_Type_Code,
  REPLACE(pcma_cdic_0237.DESCRIPTION, '|', '') AS Description
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0237` AS pcma_cdic_0237
INNER JOIN
  pcma_cdic_0237_latest_load AS pcma_cdic_0237_ll
ON
  pcma_cdic_0237.REC_LOAD_TIMESTAMP = pcma_cdic_0237_ll.LATEST_REC_LOAD_TIMESTAMP;
