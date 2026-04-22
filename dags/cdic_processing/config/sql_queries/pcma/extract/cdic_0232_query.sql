EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0232_latest_load AS (
  SELECT
    MAX(pcma_cdic_0232.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0232` AS pcma_cdic_0232 )
SELECT
  pcma_cdic_0232.REGISTERED_PLAN_TYPE_CODE AS Registered_Plan_Type_Code,
  REPLACE(pcma_cdic_0232.MI_REGISTERED_PLAN_TYPE, '|', '') AS MI_Registered_Plan_Type,
  REPLACE(pcma_cdic_0232.DESCRIPTION, '|', '') AS Description
FROM
  `pcb-{env}-curated.domain_cdic.PCMA_CDIC_0232` AS pcma_cdic_0232
INNER JOIN
  pcma_cdic_0232_latest_load AS pcma_cdic_0232_ll
ON
  pcma_cdic_0232.REC_LOAD_TIMESTAMP = pcma_cdic_0232_ll.LATEST_REC_LOAD_TIMESTAMP;