EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0121_latest_load AS (
  SELECT
    MAX(pcma_cdic_0121.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0121` AS pcma_cdic_0121 )
SELECT
  REPLACE(TO_HEX(SHA256(pcma_cdic_0121.DEPOSITOR_UNIQUE_ID)), '|', '') AS Depositor_Unique_ID,
  REPLACE(REGEXP_REPLACE(pcma_cdic_0121.PAYEE_NAME, r'[a-zA-Z0-9]', 'X'), '|', '') AS Payee_Name,
  pcma_cdic_0121.INSTITUTION_NUMBER AS Institution_Number,
  REGEXP_REPLACE(pcma_cdic_0121.TRANSIT_NUMBER, r'[0-9]', '0') AS Transit_Number,
  REGEXP_REPLACE(pcma_cdic_0121.ACCOUNT_NUMBER, r'[0-9]', '0') AS Account_Number,
  pcma_cdic_0121.CURRENCY_CODE AS Currency_Code,
  pcma_cdic_0121.JOINT_ACCOUNT_FLAG AS Joint_Account_Flag,
  pcma_cdic_0121.START_DATE AS Start_Date,
  pcma_cdic_0121.LAST_FUNDS_TRANSFER AS Last_Funds_Transfer,
  pcma_cdic_0121.LAST_OUTBOUND_FUNDS_TRANSFER AS Last_Outbound_Funds_Transfer,
  pcma_cdic_0121.NEXT_OUTBOUND_FUNDS_TRANSFER AS Next_Outbound_Funds_Transfer
FROM
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0121` AS pcma_cdic_0121
INNER JOIN
  pcma_cdic_0121_latest_load AS pcma_cdic_0121_ll
ON
  pcma_cdic_0121.REC_LOAD_TIMESTAMP = pcma_cdic_0121_ll.LATEST_REC_LOAD_TIMESTAMP;