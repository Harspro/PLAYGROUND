EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0120_latest_load AS (
  SELECT
    MAX(pcma_cdic_0120.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0120` AS pcma_cdic_0120 )
SELECT
  REPLACE(TO_HEX(SHA256(pcma_cdic_0120.Depositor_Unique_ID)), '|', '') AS Depositor_Unique_ID,
  pcma_cdic_0120.ADDRESS_COUNT AS Address_Count,
  pcma_cdic_0120.ADDRESS_TYPE_CODE AS Address_Type_Code,
  pcma_cdic_0120.PRIMARY_ADDRESS_FLAG AS Primary_Address_Flag,
  pcma_cdic_0120.ADDRESS_CHANGE AS Address_Change,
  pcma_cdic_0120.UNDELIVERABLE_FLAG AS Undeliverable_Flag,
  REPLACE(REGEXP_REPLACE(pcma_cdic_0120.ADDRESS_1, r'[a-zA-Z0-9]', 'X'),'|',' ') AS Address_1,
  REPLACE(REGEXP_REPLACE(pcma_cdic_0120.ADDRESS_2, r'[a-zA-Z0-9]', 'X'),'|',' ') AS Address_2,
  REPLACE(pcma_cdic_0120.CITY, '|', '') AS City,
  REPLACE(pcma_cdic_0120.PROVINCE, '|', '') AS Province,
  REPLACE(REGEXP_REPLACE(pcma_cdic_0120.POSTAL_CODE, r'[a-zA-Z0-9]', 'X'), '|', '') AS Postal_Code,
  REPLACE(pcma_cdic_0120.COUNTRY, '|', '') as Country
FROM
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0120` AS pcma_cdic_0120
INNER JOIN
  pcma_cdic_0120_latest_load AS pcma_cdic_0120_ll
ON
  pcma_cdic_0120.REC_LOAD_TIMESTAMP = pcma_cdic_0120_ll.LATEST_REC_LOAD_TIMESTAMP;