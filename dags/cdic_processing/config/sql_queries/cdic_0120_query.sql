CREATE OR REPLACE VIEW
  `pcb-{env}-processing.domain_cdic.CDIC_0120_VIEW` OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR),
    description = "View for CDIC 120 file" ) AS
SELECT
  ci.customer_uid,
  cdic120.*
FROM
  `pcb-{env}-curated.domain_cdic.CDIC_0100_STAGING` cdic120
INNER JOIN
  `pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` ci
ON
  ci.CUSTOMER_IDENTIFIER_NO = cdic120.DEPOSITOR_ID_LINK
WHERE
  ci.TYPE = 'PCF-CUSTOMER-ID';
CREATE OR REPLACE VIEW
  `pcb-{env}-processing.domain_cdic.CUSTOMER_0120_VIEW` OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR),
    description = "View for Customer 120 file" ) AS
SELECT
  cuv.*,
  ci.CUSTOMER_IDENTIFIER_NO
FROM
  `pcb-{env}-processing.domain_cdic.CDIC_0120_VIEW` cuv
INNER JOIN
  `pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` ci
ON
  ci.CUSTOMER_UID = cuv.CUSTOMER_UID
WHERE
  ci.TYPE = 'CDIC-DEPOSITOR-ID'
  AND ci.DISABLED_IND = 'N';
CREATE OR REPLACE VIEW
  `pcb-{env}-processing.domain_cdic.MAIN_0120_VIEW_CDIC` OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR),
    description = "Main view for CDIC 120" ) AS
SELECT
  c.DEPOSITOR_UNIQUE_ID AS DEPOSITOR_UNIQUE_ID,
  ROW_NUMBER() OVER (PARTITION BY c.CUSTOMER_UID, con.TYPE ORDER BY con.CONTACT_UID ASC ) AS ADDRESS_COUNT,
  CAST ((
    SELECT
      ADDRESS_TYPE_CODE
    FROM
      `pcb-{env}-curated.cots_cdic.CDIC_0221`
    WHERE
      MI_ADDRESS_TYPE= CONCAT(CASE
          WHEN con.CONTEXT LIKE 'TEMP%' THEN 'TEMPORARY'
          ELSE con.CONTEXT
      END
        ,'-ADDRESS') ) AS STRING) AS ADDRESS_TYPE_CODE,
  (
    CASE
      WHEN con.CONTEXT = 'PRIMARY' THEN 'Y'
      ELSE 'N'
  END
    ) AS PRIMARY_ADDRESS_FLAG,
  FORMAT_DATETIME('%Y%m%d:%H%M%S',addrCon.UPDATE_DT) AS ADDRESS_CHANGE,
  "" AS UNDELIVERABLE_FLAG,
  addrCon.ADDRESS_LINE_1 AS ADDRESS_1,
  addrCon.ADDRESS_LINE_2 AS ADDRESS_2,
  addrCon.CITY AS CITY,
  addrCon.PROVINCE_STATE AS PROVINCE,
  addrCon.POSTAL_ZIP_CODE AS POSTAL_CODE,
  CASE
    WHEN addrCon.COUNTRY = 'USA' THEN UPPER(addrCon.COUNTRY)
    ELSE UPPER(REPLACE(cdicCountry.Name,'|','/'))
END
  AS COUNTRY
FROM
  `pcb-{env}-processing.domain_cdic.CUSTOMER_0120_VIEW` c
LEFT JOIN
  `pcb-{env}-curated.domain_customer_management.CONTACT` con
ON
  con.CUSTOMER_UID = c.CUSTOMER_UID
INNER JOIN
  `pcb-{env}-curated.domain_customer_management.ADDRESS_CONTACT` addrCon
ON
  addrCon.CONTACT_UID = con.CONTACT_UID
INNER JOIN
  `pcb-{env}-curated.domain_cdic.REF_CDIC_COUNTRY` cdicCountry
ON
  addrCon.COUNTRY = cdicCountry.CODE
WHERE
  con.TYPE = 'ADDRESS';
CREATE OR REPLACE TABLE
  `pcb-{env}-curated.cots_cdic.CDIC_0120` AS
SELECT
  *
FROM
  `pcb-{env}-processing.domain_cdic.MAIN_0120_VIEW_CDIC`
LIMIT
  0;
INSERT INTO
  `pcb-{env}-curated.cots_cdic.CDIC_0120`
SELECT
  *
FROM
  `pcb-{env}-processing.domain_cdic.MAIN_0120_VIEW_CDIC`;