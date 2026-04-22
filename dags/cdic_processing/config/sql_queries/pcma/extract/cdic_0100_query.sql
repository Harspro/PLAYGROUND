EXPORT DATA
  OPTIONS ( uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE ) AS
WITH
  pcma_cdic_0100_latest_load AS (
  SELECT
    MAX(pcma_cdic_0100.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0100` AS pcma_cdic_0100 )
SELECT
  REPLACE(TO_HEX(SHA256(pcma_cdic_0100.DEPOSITOR_UNIQUE_ID)), '|', '') AS Depositor_Unique_ID,
  REPLACE(TO_HEX(SHA256(pcma_cdic_0100.DEPOSITOR_ID_LINK)), '|', '') AS Depositor_ID_Link,
  pcma_cdic_0100.SUBSYSTEM_ID AS Subsystem_ID,
  REPLACE(pcma_cdic_0100.DEPOSITOR_BRANCH, '|', '') AS Depositor_Branch,
  REPLACE(REGEXP_REPLACE(pcma_cdic_0100.DEPOSITOR_ID, r'[a-zA-Z0-9]', 'X'), '|', '') AS Depositor_ID,
  REPLACE(REGEXP_REPLACE(pcma_cdic_0100.NAME_PREFIX, r'[a-zA-Z0-9]', 'X'), '|', '') AS Name_Prefix,
  REPLACE(REGEXP_REPLACE(pcma_cdic_0100.NAME, r'[a-zA-Z0-9]', 'X'), '|', '') AS Name,
  REPLACE(REGEXP_REPLACE(pcma_cdic_0100.FIRST_NAME, r'[a-zA-Z0-9]', 'X'), '|', '') AS First_Name,
  REPLACE(REGEXP_REPLACE(pcma_cdic_0100.MIDDLE_NAME, r'[a-zA-Z0-9]', 'X'), '|', '') AS Middle_Name,
  REPLACE(REGEXP_REPLACE(pcma_cdic_0100.LAST_NAME, r'[a-zA-Z0-9]', 'X'), '|', '') AS Last_Name,
  REPLACE(REGEXP_REPLACE(pcma_cdic_0100.NAME_SUFFIX, r'[a-zA-Z0-9]', 'X'), '|', '') AS Name_Suffix,
  '19000101' AS Birth_Date,
  REPLACE(REGEXP_REPLACE(pcma_cdic_0100.PHONE_1, r'[a-zA-Z0-9]', 'X'), '|', '') AS Phone_1,
  REPLACE(REGEXP_REPLACE(pcma_cdic_0100.PHONE_2, r'[a-zA-Z0-9]', 'X'), '|', '') AS Phone_2,
  REPLACE(REGEXP_REPLACE(pcma_cdic_0100.EMAIL, r'[a-zA-Z0-9]', 'X'), '|', '') AS Email,
  pcma_cdic_0100.DEPOSITOR_TYPE_CODE AS Depositor_Type_Code,
  pcma_cdic_0100.DEPOSITOR_AGENT_FLAG AS Depositor_Agent_Flag,
  pcma_cdic_0100.LANGUAGE_FLAG AS Language_Flag,
  pcma_cdic_0100.EMPLOYEE_FLAG AS Employee_Flag,
  pcma_cdic_0100.PHONE_1_TYPE AS Phone_1_Type,
  pcma_cdic_0100.PHONE_2_TYPE AS Phone_2_Type,
  pcma_cdic_0100.MI_RESPONSIBLE_PARTY_FLAG AS MI_Responsible_Party_Flag,
  REPLACE(pcma_cdic_0100.NON_RESIDENT_COUNTRY_CODE, '|', '') AS Non_Resident_Country_Code
FROM
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0100` AS pcma_cdic_0100
INNER JOIN
  pcma_cdic_0100_latest_load AS pcma_cdic_0100_ll
ON
  pcma_cdic_0100.REC_LOAD_TIMESTAMP = pcma_cdic_0100_ll.LATEST_REC_LOAD_TIMESTAMP;