EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0500_latest_load AS (
  SELECT
    MAX( pcma_cdic_0500.REC_LOAD_TIMESTAMP ) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0500` AS pcma_cdic_0500 )
SELECT
  REPLACE(TO_HEX(SHA256(pcma_cdic_0500.DEPOSITOR_UNIQUE_ID)), '|', '') AS Depositor_Unique_ID,
  REPLACE(TO_HEX(SHA256(pcma_cdic_0500.ACCOUNT_UNIQUE_ID)), '|', '') AS Account_Unique_ID,
  pcma_cdic_0500.RELATIONSHIP_TYPE_CODE AS Relationship_Type_Code,
  pcma_cdic_0500.PRIMARY_ACCOUNT_HOLDER_FLAG AS Primary_Account_Holder_Flag,
  pcma_cdic_0500.PAYEE_FLAG AS Payee_Flag
FROM
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0500` AS pcma_cdic_0500
INNER JOIN
  pcma_cdic_0500_latest_load AS pcma_cdic_0500_ll
ON
  pcma_cdic_0500.REC_LOAD_TIMESTAMP = pcma_cdic_0500_ll.LATEST_REC_LOAD_TIMESTAMP;