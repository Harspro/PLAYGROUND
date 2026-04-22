INSERT INTO
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0120`
WITH
  pcma_cdic_0500_latest_load AS (
  SELECT
    MAX(pcma_cdic_0500.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0500` AS pcma_cdic_0500 ),
  pcma_cdic_0221_latest_load AS (
  SELECT
    MAX(pcma_cdic_0221.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0221` AS pcma_cdic_0221 ),
  pcma_cdic_0221_result AS (
  SELECT
    pcma_cdic_0221.ADDRESS_TYPE_CODE,
    pcma_cdic_0221.MI_ADDRESS_TYPE
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0221` AS pcma_cdic_0221
  INNER JOIN
    pcma_cdic_0221_latest_load AS pcma_cdic_0221_ll
  ON
    (pcma_cdic_0221.REC_LOAD_TIMESTAMP = pcma_cdic_0221_ll.LATEST_REC_LOAD_TIMESTAMP)),
  contact_data AS (
  SELECT
    DISTINCT pcma_cdic_0500.Depositor_Unique_ID,
    co.CONTACT_UID,
    co.TYPE,
    co.CONTEXT
  FROM
    `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0500` AS pcma_cdic_0500
  INNER JOIN
    pcma_cdic_0500_latest_load AS pcma_cdic_0500_ll
  ON
    (pcma_cdic_0500.REC_LOAD_TIMESTAMP = pcma_cdic_0500_ll.LATEST_REC_LOAD_TIMESTAMP)
  INNER JOIN
    `pcb-{env}-curated.domain_customer_management.CONTACT` AS co
  ON
    pcma_cdic_0500.Depositor_Unique_ID = CAST(co.CUSTOMER_UID AS STRING)),
  address_data AS (
  SELECT
    cod.Depositor_Unique_ID,
    pcma_cdic_0221.ADDRESS_TYPE_CODE,
    FORMAT_DATETIME('%Y%m%d:%H%M%S',ac.UPDATE_DT) ADDRESS_CHANGE,
    ac.ADDRESS_LINE_1 ADDRESS_1,
    ac.ADDRESS_LINE_2 ADDRESS_2,
    ac.CITY CITY,
    ac.PROVINCE_STATE PROVINCE,
    ac.POSTAL_ZIP_CODE POSTAL_CODE,
    ac.COUNTRY COUNTRY,
    'Y' PRIMARY_ADDRESS_FLAG
  FROM
    contact_data AS cod
  INNER JOIN
    `pcb-{env}-curated.domain_customer_management.ADDRESS_CONTACT` AS ac
  ON
    (cod.CONTACT_UID = ac.CONTACT_UID
      AND LOWER(cod.TYPE) = 'address'
      AND LOWER(cod.CONTEXT) = 'primary')
  INNER JOIN
    pcma_cdic_0221_result AS pcma_cdic_0221
  ON
    (LOWER(pcma_cdic_0221.MI_ADDRESS_TYPE) = 'primary-address')
  UNION ALL
  SELECT
    cod.Depositor_Unique_ID,
    pcma_cdic_0221.ADDRESS_TYPE_CODE,
    FORMAT_DATETIME('%Y%m%d:%H%M%S',ac.UPDATE_DT) ADDRESS_CHANGE,
    ac.ADDRESS_LINE_1 ADDRESS_1,
    ac.ADDRESS_LINE_2 ADDRESS_2,
    ac.CITY CITY,
    ac.PROVINCE_STATE PROVINCE,
    ac.POSTAL_ZIP_CODE POSTAL_CODE,
    ac.COUNTRY COUNTRY,
    'N' PRIMARY_ADDRESS_FLAG
  FROM
    contact_data AS cod
  INNER JOIN
    `pcb-{env}-curated.domain_customer_management.ADDRESS_CONTACT` AS ac
  ON
    ( cod.CONTACT_UID = ac.CONTACT_UID
      AND LOWER(cod.TYPE) = 'address'
      AND LOWER(cod.context) = 'mailing' )
  INNER JOIN
    pcma_cdic_0221_result AS pcma_cdic_0221
  ON
    ( LOWER(pcma_cdic_0221.MI_ADDRESS_TYPE) = 'mailing-address' )
  UNION ALL
  SELECT
    cod.Depositor_Unique_ID,
    pcma_cdic_0221.ADDRESS_TYPE_CODE,
    FORMAT_DATETIME('%Y%m%d:%H%M%S',ac.UPDATE_DT) ADDRESS_CHANGE,
    ac.ADDRESS_LINE_1 ADDRESS_1,
    ac.ADDRESS_LINE_2 ADDRESS_2,
    ac.CITY CITY,
    ac.PROVINCE_STATE PROVINCE,
    ac.POSTAL_ZIP_CODE POSTAL_CODE,
    ac.COUNTRY COUNTRY,
    'N' PRIMARY_ADDRESS_FLAG
  FROM
    contact_data AS cod
  INNER JOIN
    `pcb-{env}-curated.domain_customer_management.ADDRESS_CONTACT` AS ac
  ON
    ( cod.CONTACT_UID = ac.CONTACT_UID
      AND LOWER(cod.TYPE) = 'address'
      AND LOWER(cod.context) = 'business' )
  INNER JOIN
    pcma_cdic_0221_result AS pcma_cdic_0221
  ON
    ( LOWER(pcma_cdic_0221.MI_ADDRESS_TYPE) = 'business-address' )
  UNION ALL
  SELECT
    cod.Depositor_Unique_ID,
    pcma_cdic_0221.ADDRESS_TYPE_CODE,
    FORMAT_DATETIME('%Y%m%d:%H%M%S',ac.UPDATE_DT) AS ADDRESS_CHANGE,
    ac.ADDRESS_LINE_1 AS ADDRESS_1,
    ac.ADDRESS_LINE_2 AS ADDRESS_2,
    ac.CITY,
    ac.PROVINCE_STATE AS PROVINCE,
    ac.POSTAL_ZIP_CODE AS POSTAL_CODE,
    ac.COUNTRY,
    'N' AS PRIMARY_ADDRESS_FLAG
  FROM
    contact_data AS cod
  INNER JOIN
    `pcb-{env}-curated.domain_customer_management.ADDRESS_CONTACT` AS ac
  ON
    ( cod.CONTACT_UID = ac.CONTACT_UID
      AND LOWER(cod.TYPE) = 'address'
      AND LOWER(cod.context) = 'temp-mailing' )
  INNER JOIN
    pcma_cdic_0221_result AS pcma_cdic_0221
  ON
    ( LOWER(pcma_cdic_0221.MI_ADDRESS_TYPE) = 'temporary-address' )
  INNER JOIN
    `pcb-{env}-curated.domain_customer_management.ALTERNATE_CONTACT` AS alc
  ON
    cod.CONTACT_UID = alc.ALT_CONTACT_UID
    AND alc.END_DT > CURRENT_DATETIME()),
  ref_cdic_country_latest_load AS (
  SELECT
    FORMAT_TIMESTAMP('%Y-%m-%d %H:00:00', MAX(cc.UPDATE_DT)) AS LatestHour
  FROM
    `pcb-{env}-curated.domain_cdic.REF_CDIC_COUNTRY` AS cc )
SELECT
  ad.Depositor_Unique_ID AS DEPOSITOR_UNIQUE_ID,
  '' AS ADDRESS_COUNT,
  CAST(ad.ADDRESS_TYPE_CODE AS STRING) AS ADDRESS_TYPE_CODE,
  ad.PRIMARY_ADDRESS_FLAG,
  ad.ADDRESS_CHANGE,
  '' AS UNDELIVERABLE_FLAG,
  ad.ADDRESS_1,
  ad.ADDRESS_2,
  ad.CITY,
  ad.PROVINCE,
  ad.POSTAL_CODE,
  UPPER(cc.NAME) AS COUNTRY,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  address_data AS ad
INNER JOIN
  `pcb-{env}-curated.domain_cdic.REF_CDIC_COUNTRY` AS cc
ON
  (LOWER(ad.COUNTRY) = LOWER(cc.CODE))
WHERE
  cc.LANGUAGE = 'en'
  AND CAST(cc.UPDATE_DT AS TIMESTAMP) >= (
  SELECT
    TIMESTAMP(cc_ll.LatestHour)
  FROM
    ref_cdic_country_latest_load AS cc_ll)
  AND CAST(cc.UPDATE_DT AS TIMESTAMP) < TIMESTAMP(FORMAT_TIMESTAMP('%Y-%m-%d %H:00:00', TIMESTAMP_ADD((
        SELECT
          TIMESTAMP(cc_ll.LatestHour)
        FROM
          ref_cdic_country_latest_load AS cc_ll), INTERVAL 1 HOUR)));