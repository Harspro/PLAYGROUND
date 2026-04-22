CREATE OR REPLACE VIEW
  `pcb-{env}-processing.domain_cdic.MAIN_VIEW_CDIC`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR),
  description = "Main view for CDIC"
) AS

  WITH cdic AS (
  SELECT
    cdic100.Depositor_ID_Link,
    ci.customer_uid,
    cdic100.Depositor_Branch,
    cdic100.Depositor_Type_Code,
    cdic100.Depositor_ID,
    cdic100.Depositor_Unique_Id,
    cdic100.Depositor_Agent_Flag
  FROM pcb-{env}-curated.domain_cdic.CDIC_0100_STAGING cdic100
  INNER JOIN pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER ci ON ci.CUSTOMER_IDENTIFIER_NO = cdic100.DEPOSITOR_ID_LINK
  WHERE ci.type = 'PCF-CUSTOMER-ID' ),

  customer AS (
  SELECT
    cuv.*,
    ci.CUSTOMER_IDENTIFIER_NO,
    c.NAME_PREFIX,
    c.GIVEN_NAME,
    c.MIDDLE_NAME,
    c.SURNAME,
    c.NAME_SUFFIX,
    c.BIRTH_DT,
    (CASE
        WHEN crp.CUSTOMER_UID IS NULL THEN 'N'
        ELSE 'Y'
    END) AS MI_Responsible_Party_Flag
  FROM cdic cuv
  INNER JOIN pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER ci ON ci.CUSTOMER_UID = cuv.customer_uid
  LEFT JOIN pcb-{env}-curated.domain_customer_management.CUSTOMER c ON c.CUSTOMER_UID = cuv.customer_uid
  LEFT JOIN pcb-{env}-curated.domain_cdic.CDIC_RESPONSIBLE_PARTY crp ON crp.CUSTOMER_UID = cuv.customer_uid
  WHERE ci.TYPE = 'PCF-CUSTOMER-ID' and ci.DISABLED_IND = 'N')

SELECT
  c.DEPOSITOR_UNIQUE_ID AS DEPOSITOR_UNIQUE_ID,
  c.CUSTOMER_IDENTIFIER_NO AS DEPOSITOR_ID_LINK,
  '003' AS SUBSYSTEM_ID,
  c.Depositor_Branch AS DEPOSITOR_BRANCH,
  c.DEPOSITOR_ID AS DEPOSITOR_ID,
  c.NAME_PREFIX AS NAME_PREFIX,
  ARRAY_TO_STRING([c.GIVEN_NAME,c.MIDDLE_NAME,c.SURNAME],' ') AS NAME,
  c.GIVEN_NAME AS FIRST_NAME,
  c.MIDDLE_NAME AS MIDDLE_NAME,
  c.SURNAME AS LAST_NAME,
  c.NAME_SUFFIX AS NAME_SUFFIX,
  FORMAT_DATE('%Y%m%d',c.BIRTH_DT) AS BIRTH_DATE,
  ARRAY_TO_STRING([primaryContact.COUNTRY_CODE,primaryContact.AREA_CODE,primaryContact.PHONE_NUMBER],'') AS PHONE_1,
  CASE
    WHEN alternateContact.PHONE_NUMBER IS NULL THEN ARRAY_TO_STRING([businessContact.COUNTRY_CODE,businessContact.AREA_CODE,businessContact.PHONE_NUMBER],'')
    ELSE ARRAY_TO_STRING([alternateContact.COUNTRY_CODE,alternateContact.AREA_CODE,alternateContact.PHONE_NUMBER],'')
  END AS PHONE_2,
  (
    SELECT
      ec.EMAIL_ADDRESS
    FROM pcb-{env}-curated.domain_customer_management.EMAIL_CONTACT ec
    INNER JOIN pcb-{env}-curated.domain_customer_management.CONTACT con ON con.CONTACT_UID = ec.CONTACT_UID
    WHERE con.CUSTOMER_UID = c.CUSTOMER_UID
      AND con.CONTEXT = 'PRIMARY'
      AND con.TYPE = 'EMAIL'
  ) AS EMAIL,
  c.Depositor_Type_Code AS DEPOSITOR_TYPE_CODE,
  c.Depositor_Agent_Flag AS DEPOSITOR_AGENT_FLAG,
  (
    SELECT
    CASE
      WHEN cpp.VALUE = 'en' THEN 'E'
      WHEN cpp.VALUE = 'fr' THEN 'F'
      ELSE 'O'
    END
    FROM pcb-{env}-curated.domain_customer_management.CUSTOMER_PREFERENCE_PARAM cpp
    INNER JOIN pcb-{env}-curated.domain_customer_management.CUSTOMER_PREFERENCE cp ON cp.CUSTOMER_PREFERENCE_UID = cpp.CUSTOMER_PREFERENCE_UID
    WHERE cp.CUSTOMER_UID = c.CUSTOMER_UID AND cp.CODE = 'LANGUAGE'
  ) AS LANGUAGE_FLAG,
  'N' AS EMPLOYEE_FLAG,
  CASE
    WHEN primaryContact.PHONE_NUMBER IS NULL THEN '1'
    WHEN primaryContact.MOBILE_IND = 'Y' THEN '2'
    ELSE '3'
  END AS PHONE_1_TYPE,
  CASE
    WHEN businessContact.PHONE_NUMBER IS NULL and alternateContact.PHONE_NUMBER IS NULL THEN '1'
    WHEN alternateContact.PHONE_NUMBER IS NOT NULL and IFNULL(alternateContact.MOBILE_IND,'N') = 'Y' THEN '2'
    WHEN alternateContact.PHONE_NUMBER IS NOT NULL and IFNULL(alternateContact.MOBILE_IND,'N') = 'N' THEN '3'
    WHEN businessContact.PHONE_NUMBER IS NOT NULL THEN '4'
  END AS PHONE_2_TYPE,
  c.MI_Responsible_Party_Flag AS MI_RESPONSIBLE_PARTY_FLAG,
  NULL AS NON_RESIDENT_COUNTRY_CODE
FROM customer c
LEFT JOIN (
          SELECT
            pc.COUNTRY_CODE,
            pc.AREA_CODE,
            pc.PHONE_NUMBER,
            pc.MOBILE_IND,
            con.CUSTOMER_UID AS CUSTOMER_UID
          FROM pcb-{env}-curated.domain_customer_management.PHONE_CONTACT pc
          INNER JOIN pcb-{env}-curated.domain_customer_management.CONTACT con ON con.CONTACT_UID = pc.CONTACT_UID
          WHERE
            con.CONTEXT = 'PRIMARY'
            AND con.TYPE = 'PHONE'
          ) primaryContact ON primaryContact.CUSTOMER_UID = c.CUSTOMER_UID
LEFT JOIN (
          SELECT
            pc.COUNTRY_CODE,
            pc.AREA_CODE,
            pc.PHONE_NUMBER,
            pc.MOBILE_IND,
            con.CUSTOMER_UID AS CUSTOMER_UID
          FROM pcb-{env}-curated.domain_customer_management.PHONE_CONTACT pc
          INNER JOIN pcb-{env}-curated.domain_customer_management.CONTACT con ON con.CONTACT_UID = pc.CONTACT_UID
          WHERE
            con.CONTEXT = 'BUSINESS'
            AND con.TYPE = 'PHONE'
          ) businessContact ON businessContact.CUSTOMER_UID = c.CUSTOMER_UID
LEFT JOIN (
          SELECT
            pc.COUNTRY_CODE,
            pc.AREA_CODE,
            pc.PHONE_NUMBER,
            pc.MOBILE_IND,
            con.CUSTOMER_UID AS CUSTOMER_UID
          FROM pcb-{env}-curated.domain_customer_management.PHONE_CONTACT pc
          INNER JOIN pcb-{env}-curated.domain_customer_management.CONTACT con ON con.CONTACT_UID = pc.CONTACT_UID
          WHERE
            con.CONTEXT = 'ALTERNATE'
            AND con.TYPE = 'PHONE'
          ) alternateContact ON alternateContact.CUSTOMER_UID = c.CUSTOMER_UID;

CREATE OR REPLACE TABLE
  `pcb-{env}-curated.cots_cdic.CDIC_0100`
AS
  SELECT
    *
  FROM pcb-{env}-processing.domain_cdic.MAIN_VIEW_CDIC
  LIMIT 0;

INSERT INTO
  `pcb-{env}-curated.cots_cdic.CDIC_0100`
SELECT
  *
FROM pcb-{env}-processing.domain_cdic.MAIN_VIEW_CDIC;