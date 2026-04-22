INSERT INTO
  `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0100`
  WITH
    pcma_cdic_0500_latest_load AS (
    SELECT
      MAX(pcma_cdic_0500.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0500` AS pcma_cdic_0500),
    latest_pcma_cdic_0500 AS (
    SELECT
      DISTINCT pcma_cdic_0500.DEPOSITOR_UNIQUE_ID,
      pcma_cdic_0500.CUSTOMER_IDENTIFIER_NO
    FROM
      `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0500` AS pcma_cdic_0500
    INNER JOIN
      pcma_cdic_0500_latest_load AS pcma_cdic_0500_ll
    ON
      pcma_cdic_0500.REC_LOAD_TIMESTAMP = pcma_cdic_0500_ll.LATEST_REC_LOAD_TIMESTAMP ),
    customer AS (
    SELECT
      pcma_cdic_0500.DEPOSITOR_UNIQUE_ID,
      pcma_cdic_0500.CUSTOMER_IDENTIFIER_NO,
      ct.NAME_PREFIX,
      ct.GIVEN_NAME,
      ct.MIDDLE_NAME,
      ct.SURNAME,
      ct.NAME_SUFFIX,
      ct.BIRTH_DT,
      crp.CUSTOMER_UID AS CRP_CUSTOMER_UID
    FROM
      latest_pcma_cdic_0500 AS pcma_cdic_0500
    LEFT JOIN
      `pcb-{env}-curated.domain_customer_management.CUSTOMER` AS ct
    ON
      pcma_cdic_0500.DEPOSITOR_UNIQUE_ID = CAST(ct.CUSTOMER_UID AS STRING)
    LEFT JOIN
      `pcb-{env}-curated.domain_cdic.CDIC_RESPONSIBLE_PARTY` AS crp
    ON
      pcma_cdic_0500.DEPOSITOR_UNIQUE_ID = CAST(crp.CUSTOMER_UID AS STRING) ),
    phone_primary AS (
    SELECT
      ph_con.COUNTRY_CODE,
      ph_con.AREA_CODE,
      ph_con.PHONE_NUMBER,
      ph_con.MOBILE_IND,
      CAST(con.CUSTOMER_UID AS STRING) AS CUSTOMER_UID
    FROM
      `pcb-{env}-curated.domain_customer_management.PHONE_CONTACT` AS ph_con
    INNER JOIN
      `pcb-{env}-curated.domain_customer_management.CONTACT` AS con
    ON
      con.CONTACT_UID = ph_con.CONTACT_UID
    WHERE
      LOWER(con.CONTEXT) = 'primary'
      AND LOWER(con.TYPE) = 'phone'),
    phone_alternate AS (
    SELECT
      ph_con.COUNTRY_CODE,
      ph_con.AREA_CODE,
      ph_con.PHONE_NUMBER,
      ph_con.MOBILE_IND,
      CAST(con.CUSTOMER_UID AS STRING) AS CUSTOMER_UID
    FROM
      `pcb-{env}-curated.domain_customer_management.PHONE_CONTACT` AS ph_con
    INNER JOIN
      `pcb-{env}-curated.domain_customer_management.CONTACT` AS con
    ON
      con.CONTACT_UID = ph_con.CONTACT_UID
    WHERE
      LOWER(con.CONTEXT) = 'alternate'
      AND LOWER(con.TYPE) = 'phone' ),
    phone_business AS (
    SELECT
      ph_con.COUNTRY_CODE,
      ph_con.AREA_CODE,
      ph_con.PHONE_NUMBER,
      ph_con.MOBILE_IND,
      CAST(con.CUSTOMER_UID AS STRING) AS CUSTOMER_UID
    FROM
      `pcb-{env}-curated.domain_customer_management.PHONE_CONTACT` AS ph_con
    INNER JOIN
      `pcb-{env}-curated.domain_customer_management.CONTACT` AS con
    ON
      con.CONTACT_UID = ph_con.CONTACT_UID
    WHERE
      LOWER(con.CONTEXT) = 'business'
      AND LOWER(con.TYPE) = 'phone' ),
    email AS (
    SELECT
      CAST(con.CUSTOMER_UID AS STRING) AS CUSTOMER_UID,
      eml_con.EMAIL_ADDRESS
    FROM
      `pcb-{env}-curated.domain_customer_management.EMAIL_CONTACT` AS eml_con
    INNER JOIN
      `pcb-{env}-curated.domain_customer_management.CONTACT` AS con
    ON
      con.CONTACT_UID = eml_con.CONTACT_UID
    WHERE
      LOWER(con.CONTEXT) = 'primary'
      AND LOWER(con.TYPE) = 'email' )
  SELECT
    c.DEPOSITOR_UNIQUE_ID,
    c.CUSTOMER_IDENTIFIER_NO AS DEPOSITOR_ID_LINK,
    (
    SELECT
      ss_id.SUBSYSTEM_ID
    FROM (
      SELECT
        pcma_cdic_0999.SUBSYSTEM_ID,
        ROW_NUMBER() OVER (PARTITION BY pcma_cdic_0999.MI_SUBSYSTEM_CODE ORDER BY pcma_cdic_0999.REC_LOAD_TIMESTAMP DESC) AS REC_RANK
      FROM
        `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0999` AS pcma_cdic_0999
      WHERE
        LOWER(pcma_cdic_0999.MI_SUBSYSTEM_CODE) = 'pcb-deposits-1') AS ss_id
    WHERE
      ss_id.REC_RANK=1) AS SUBSYSTEM_ID,
    'CA0010001' AS DEPOSITOR_BRANCH,
    c.DEPOSITOR_UNIQUE_ID AS DEPOSITOR_ID,
    c.NAME_PREFIX,
    CONCAT(COALESCE(c.GIVEN_NAME||' ',''),COALESCE(c.MIDDLE_NAME||' ',''),COALESCE(c.SURNAME,'')) AS NAME,
    c.GIVEN_NAME AS FIRST_NAME,
    c.MIDDLE_NAME,
    c.SURNAME AS LAST_NAME,
    c.NAME_SUFFIX,
    CAST(c.BIRTH_DT AS STRING) AS BIRTH_DATE,
    ARRAY_TO_STRING([ph_p.COUNTRY_CODE,ph_p.AREA_CODE,ph_p.PHONE_NUMBER],'') AS PHONE_1,
    CASE
      WHEN ph_alt.PHONE_NUMBER IS NULL THEN ARRAY_TO_STRING([ph_b.COUNTRY_CODE,ph_b.AREA_CODE,ph_b.PHONE_NUMBER],'')
      ELSE ARRAY_TO_STRING([ph_alt.COUNTRY_CODE,ph_alt.AREA_CODE,ph_alt.PHONE_NUMBER],'')
  END
    AS PHONE_2,
    eml.EMAIL_ADDRESS AS EMAIL,
    (
    SELECT
      dtc.DEPOSITOR_TYPE_CODE
    FROM (
      SELECT
        pcma_cdic_0201.DEPOSITOR_TYPE_CODE,
        ROW_NUMBER() OVER (PARTITION BY pcma_cdic_0201.MI_DEPOSITOR_TYPE ORDER BY pcma_cdic_0201.REC_LOAD_TIMESTAMP DESC) AS REC_RANK
      FROM
        `pcb-{env}-curated.cots_cdic.PCMA_CDIC_0201` AS pcma_cdic_0201
      WHERE
        LOWER(pcma_cdic_0201.MI_DEPOSITOR_TYPE)='pcb-individual') AS dtc
    WHERE
      dtc.REC_RANK=1) AS DEPOSITOR_TYPE_CODE,
    'N' AS DEPOSITOR_AGENT_FLAG,
    CASE
      WHEN LOWER(ct_prf_prm.VALUE)= 'en' THEN 'E'
      WHEN LOWER(ct_prf_prm.VALUE)= 'fr' THEN 'F'
      ELSE 'O'
  END
    AS LANGUAGE_FLAG,
    CAST(NULL AS STRING) AS EMPLOYEE_FLAG,
    CASE
      WHEN ph_p.PHONE_NUMBER IS NULL THEN '1'
      WHEN ph_p.MOBILE_IND = 'Y' THEN '2'
      ELSE '3'
  END
    AS PHONE_1_TYPE,
    CASE
      WHEN ph_b.PHONE_NUMBER IS NULL AND ph_alt.PHONE_NUMBER IS NULL THEN '1'
      WHEN ph_alt.PHONE_NUMBER IS NOT NULL
    AND IFNULL(ph_alt.MOBILE_IND,'N') = 'Y' THEN '2'
      WHEN ph_alt.PHONE_NUMBER IS NOT NULL AND IFNULL(ph_alt.MOBILE_IND,'N') = 'N' THEN '3'
      WHEN ph_b.PHONE_NUMBER IS NOT NULL THEN '4'
  END
    AS PHONE_2_TYPE,
    CASE
      WHEN c.CRP_CUSTOMER_UID IS NULL THEN 'N'
      ELSE 'Y'
  END
    AS MI_RESPONSIBLE_PARTY_FLAG,
    CAST(NULL AS STRING) AS NON_RESIDENT_COUNTRY_CODE,
    CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
    '{dag_id}' AS JOB_ID
  FROM
    customer AS c
  LEFT JOIN
    PHONE_PRIMARY AS ph_p
  ON
    c.DEPOSITOR_UNIQUE_ID = ph_p.CUSTOMER_UID
  LEFT JOIN
    PHONE_ALTERNATE AS ph_alt
  ON
    c.DEPOSITOR_UNIQUE_ID = ph_alt.CUSTOMER_UID
  LEFT JOIN
    PHONE_BUSINESS AS ph_b
  ON
    c.DEPOSITOR_UNIQUE_ID = ph_b.CUSTOMER_UID
  LEFT JOIN
    email AS eml
  ON
    c.DEPOSITOR_UNIQUE_ID = eml.CUSTOMER_UID
  LEFT JOIN (
    SELECT
      latest_cp.CUSTOMER_UID,
      latest_cp.CUSTOMER_PREFERENCE_UID
    FROM (
      SELECT
        cp.CUSTOMER_UID,
        cp.CUSTOMER_PREFERENCE_UID,
        ROW_NUMBER() OVER (PARTITION BY cp.CUSTOMER_UID ORDER BY cp.CREATE_DT DESC) AS REC_RANK
      FROM
        `pcb-{env}-curated.domain_customer_management.CUSTOMER_PREFERENCE` AS cp) AS latest_cp
    WHERE
      latest_cp.REC_RANK =1) AS ct_prf
  ON
    c.DEPOSITOR_UNIQUE_ID = CAST(ct_prf.CUSTOMER_UID AS STRING)
  LEFT JOIN
    `pcb-{env}-curated.domain_customer_management.CUSTOMER_PREFERENCE_PARAM` AS ct_prf_prm
  ON
    ct_prf.CUSTOMER_PREFERENCE_UID = ct_prf_prm.CUSTOMER_PREFERENCE_UID