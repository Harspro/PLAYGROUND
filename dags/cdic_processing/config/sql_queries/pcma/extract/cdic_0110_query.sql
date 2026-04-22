EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0110_latest_load AS (
  SELECT
    MAX( pcma_cdic_0110.REC_LOAD_TIMESTAMP ) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0110` AS pcma_cdic_0110 )
SELECT
  REPLACE(TO_HEX(SHA256(pcma_cdic_0110.DEPOSITOR_UNIQUE_ID)), '|', '') AS Depositor_Unique_ID,
  pcma_cdic_0110.PERSONAL_ID_COUNT AS Personal_ID_Count,
  REPLACE(REGEXP_REPLACE(pcma_cdic_0110.IDENTIFICATION_NUMBER, r'[a-zA-Z0-9]', 'X'),'|', '') AS Identification_Number,
  pcma_cdic_0110.PERSONAL_ID_TYPE_CODE AS Personal_ID_Type_Code
FROM
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0110` AS pcma_cdic_0110
INNER JOIN
  pcma_cdic_0110_latest_load AS pcma_cdic_0110_ll
ON
  pcma_cdic_0110.REC_LOAD_TIMESTAMP = pcma_cdic_0110_ll.LATEST_REC_LOAD_TIMESTAMP;