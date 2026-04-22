EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0900_latest_load AS (
  SELECT
    MAX(pcma_cdic_0900.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0900` AS pcma_cdic_0900 )
SELECT
  REPLACE(TO_HEX(SHA256(pcma_cdic_0900.ACCOUNT_UNIQUE_ID)), '|', '') AS Account_Unique_ID,
  pcma_cdic_0900.SUBSYSTEM_ID AS Subsystem_ID,
  pcma_cdic_0900.LAST_INTEREST_PAYMENT_DATE AS Last_Interest_Payment_Date,
  pcma_cdic_0900.INTEREST_ACCRUED_AMOUNT AS Interest_Accrued_Amount,
  pcma_cdic_0900.CURRENCY_CODE AS Currency_Code
FROM
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0900` pcma_cdic_0900
INNER JOIN
  pcma_cdic_0900_latest_load AS pcma_cdic_0900_ll
ON
  pcma_cdic_0900.REC_LOAD_TIMESTAMP = pcma_cdic_0900_ll.LATEST_REC_LOAD_TIMESTAMP;