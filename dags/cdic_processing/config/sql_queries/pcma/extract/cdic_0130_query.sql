EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0130_latest_load AS (
  SELECT
    MAX( pcma_cdic_0130.REC_LOAD_TIMESTAMP ) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0130` AS pcma_cdic_0130 )
SELECT
  REPLACE(TO_HEX(SHA256(pcma_cdic_0130.ACCOUNT_UNIQUE_ID)), '|', '') AS Account_Unique_ID,
  REPLACE(TO_HEX(SHA256(pcma_cdic_0130.ACCOUNT_NUMBER)), '|', '') AS Account_Number,
  REPLACE(pcma_cdic_0130.ACCOUNT_BRANCH, '|', '') AS Account_Branch,
  pcma_cdic_0130.PRODUCT_CODE AS Product_Code,
  pcma_cdic_0130.REGISTERED_PLAN_TYPE_CODE AS Registered_Plan_Type_Code,
  REPLACE(pcma_cdic_0130.REGISTERED_PLAN_NUMBER, '|', '') AS Registered_Plan_Number,
  pcma_cdic_0130.CURRENCY_CODE AS Currency_Code,
  pcma_cdic_0130.INSURANCE_DETERMINATION_CATEGORY_TYPE_CODE AS Insurance_Determination_Category_Type_Code,
  pcma_cdic_0130.ACCOUNT_BALANCE AS Account_Balance,
  pcma_cdic_0130.ACCESSIBLE_BALANCE AS Accessible_Balance,
  pcma_cdic_0130.MATURITY_DATE AS Maturity_Date,
  pcma_cdic_0130.ACCOUNT_STATUS_CODE AS Account_Status_Code,
  pcma_cdic_0130.TRUST_ACCOUNT_TYPE_CODE AS Trust_Account_Type_Code,
  pcma_cdic_0130.CDIC_HOLD_STATUS_CODE AS CDIC_Hold_Status_Code,
  pcma_cdic_0130.JOINT_ACCOUNT_FLAG AS Joint_Account_Flag,
  pcma_cdic_0130.CLEARING_ACCOUNT_CODE AS Clearing_Account_Code,
  pcma_cdic_0130.ACCOUNT_TYPE_CODE AS Account_Type_Code,
  pcma_cdic_0130.MI_ISSUED_REGISTERED_ACCOUNT_FLAG AS MI_Issued_Registered_Account_Flag,
  pcma_cdic_0130.MI_RELATED_DEPOSIT_FLAG AS MI_Related_Deposit_Flag
FROM
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0130` AS pcma_cdic_0130
INNER JOIN
  pcma_cdic_0130_latest_load AS pcma_cdic_0130_ll
ON
  pcma_cdic_0130.REC_LOAD_TIMESTAMP = pcma_cdic_0130_ll.LATEST_REC_LOAD_TIMESTAMP;