CREATE OR REPLACE VIEW
   `pcb-{env}-processing.domain_cdic.CDIC_IDV_HISTORY_VIEW`
   OPTIONS (
       expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR),
       description = "View for CDIC IDV History"
   ) AS
   SELECT
         ci.customer_uid,
         cdicIdv.depositor_unique_id,
         cdicIdv.identification_number,
         cdicIdv.personal_id_type_code
       FROM
         `pcb-{env}-curated.domain_cdic.CDIC_IDV_HISTORY` cdicIdv
       INNER JOIN
         `pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` ci
       ON
         ci.CUSTOMER_IDENTIFIER_NO = SUBSTR(cdicIdv.DEPOSITOR_UNIQUE_ID,3)
       WHERE
         ci.TYPE = 'CDIC-DEPOSITOR-ID' and ci.DISABLED_IND = 'N';


CREATE OR REPLACE VIEW
  `pcb-{env}-processing.domain_cdic.CDIC_IDV_HISTORY_CUSTOMER_IDENTIFIER_VIEW`
  OPTIONS (
         expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR),
         description = "View for CDIC IDV History to Customer"
     ) AS
    SELECT
         ci.CUSTOMER_IDENTIFIER_NO,
         cdicIdv.IDENTIFICATION_NUMBER as IDENTIFICATION_NUMBER,
         cdicIdv.PERSONAL_ID_TYPE_CODE as PERSONAL_ID_TYPE_CODE
       FROM
          `pcb-{env}-processing.domain_cdic.CDIC_IDV_HISTORY_VIEW` cdicIdv
       INNER JOIN
         `pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` ci
       ON
         ci.CUSTOMER_UID = cdicIdv.CUSTOMER_UID
       WHERE
         ci.TYPE = 'PCF-CUSTOMER-ID';

CREATE OR REPLACE VIEW
  `pcb-{env}-processing.domain_cdic.APPLICANT_IDENTIFIER_VIEW`
  OPTIONS (
         expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR),
         description = "View for Applicant Identifier"
     ) AS
    SELECT
         api.CUSTOMER_NO AS CUSTOMER_IDENTIFIER_NO,
         CAST(CASE
                 WHEN TRIM(api.APPLICANT_IDENTIFIER_TYPE)='CAN-PASSPORT' AND (TRIM(IFNULL(api.NATIONALITY,''))='CAD' OR api.NATIONALITY IS NULL) THEN 2
                 WHEN TRIM(api.APPLICANT_IDENTIFIER_TYPE)='CAN-PASSPORT' AND TRIM(IFNULL(api.NATIONALITY,''))<>'CAD' AND api.NATIONALITY IS NOT NULL THEN 14
                 WHEN TRIM(api.APPLICANT_IDENTIFIER_TYPE)='DRIVER-LICENSE' THEN 4
                 WHEN TRIM(api.APPLICANT_IDENTIFIER_TYPE)='CAN-PERMANENT-RESIDENT-CARD' THEN 5
                 WHEN TRIM(api.APPLICANT_IDENTIFIER_TYPE)='CAN-CITIZENSHIP-CARD' THEN 6
                 WHEN TRIM(api.APPLICANT_IDENTIFIER_TYPE)='CAN-CERT-OF-INDIAN-STATUS' THEN 7
                 WHEN TRIM(api.APPLICANT_IDENTIFIER_TYPE)='HEALTH-CARD' THEN 26
                 WHEN TRIM(api.APPLICANT_IDENTIFIER_TYPE)='SERVICE-CARD' THEN 31
                 WHEN TRIM(api.APPLICANT_IDENTIFIER_TYPE)='NATIONAL-IDENTITY-CARD' AND TRIM(IFNULL(api.NATIONALITY,''))<>'CAD' AND api.NATIONALITY IS NOT NULL THEN 36
                 WHEN TRIM(api.APPLICANT_IDENTIFIER_TYPE)='NATIONAL-IDENTITY-CARD' AND (TRIM(IFNULL(api.NATIONALITY,''))='CAD' OR api.NATIONALITY IS NULL) THEN 37
                 WHEN TRIM(api.APPLICANT_IDENTIFIER_TYPE)='FOREIGN-PASSPORT' AND TRIM(IFNULL(api.NATIONALITY,''))<>'CAD' AND api.NATIONALITY IS NOT NULL THEN 14
                 WHEN TRIM(api.APPLICANT_IDENTIFIER_TYPE)='FOREIGN-PASSPORT' AND (TRIM(IFNULL(api.NATIONALITY,''))='CAD' OR api.NATIONALITY IS NULL) THEN 2
                 WHEN TRIM(api.APPLICANT_IDENTIFIER_TYPE)='PASSPORT' AND (TRIM(IFNULL(api.NATIONALITY,''))='CAD' OR api.NATIONALITY IS NULL) THEN 2
                 WHEN TRIM(api.APPLICANT_IDENTIFIER_TYPE)='PASSPORT' AND TRIM(IFNULL(api.NATIONALITY,''))<>'CAD' AND api.NATIONALITY IS NOT NULL THEN 14
                 WHEN TRIM(api.APPLICANT_IDENTIFIER_TYPE)='NOT-MATCHED' THEN -1
                 WHEN TRIM(api.APPLICANT_IDENTIFIER_TYPE)='BANK-CARD-NUMBER' THEN -1
                ELSE 0
             END AS STRING) AS PERSONAL_ID_TYPE_CODE,
         api.APPLICANT_IDENTIFIER_VALUE AS IDENTIFICATION_NUMBER
       FROM
          `pcb-{env}-curated.domain_validation_verification.APPLICANT_PERSONAL_IDENTIFIER` api
         WHERE
           UPPER(TRIM(api.APPLICANT_IDENTIFIER_TYPE)) <> 'NOT-MATCHED'
           AND UPPER(TRIM(api.APPLICANT_IDENTIFIER_TYPE)) <> 'BANK-CARD-NUMBER'
    UNION DISTINCT
        SELECT
            CUSTOMER_IDENTIFIER_NO,
            PERSONAL_ID_TYPE_CODE,
            IDENTIFICATION_NUMBER
        FROM `pcb-{env}-processing.domain_cdic.CDIC_IDV_HISTORY_CUSTOMER_IDENTIFIER_VIEW`
        WHERE PERSONAL_ID_TYPE_CODE <> '-1';

 CREATE OR REPLACE VIEW
       `pcb-{env}-processing.domain_cdic.CUSTOMER_0110_VIEW`
   OPTIONS (
       expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR),
       description = "View for Customer 110 file"
   ) AS
   SELECT
           cuv.*,
           ci.CUSTOMER_IDENTIFIER_NO
         FROM
           `pcb-{env}-curated.domain_cdic.CDIC_0100_STAGING` cuv
         INNER JOIN
           `pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` ci
         ON
           ci.CUSTOMER_IDENTIFIER_NO = cuv.DEPOSITOR_ID_LINK
         WHERE
           ci.TYPE = 'PCF-CUSTOMER-ID';

 CREATE OR REPLACE VIEW
       `pcb-{env}-processing.domain_cdic.MAIN_0110_VIEW_CDIC`
   OPTIONS (
       expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR),
       description = "Main View for 110 file"
   ) AS
   SELECT
    c.DEPOSITOR_UNIQUE_ID AS DEPOSITOR_UNIQUE_ID,
    "" as PERSONAL_ID_COUNT,
    api.IDENTIFICATION_NUMBER as IDENTIFICATION_NUMBER,
    api.PERSONAL_ID_TYPE_CODE as PERSONAL_ID_TYPE_CODE
   FROM
    `pcb-{env}-processing.domain_cdic.CUSTOMER_0110_VIEW` c
   INNER JOIN
    `pcb-{env}-processing.domain_cdic.APPLICANT_IDENTIFIER_VIEW` api
   ON
    c.CUSTOMER_IDENTIFIER_NO = api.CUSTOMER_IDENTIFIER_NO;


CREATE OR REPLACE TABLE
  `pcb-{env}-curated.cots_cdic.CDIC_0110` AS
SELECT
  *
FROM
  `pcb-{env}-processing.domain_cdic.MAIN_0110_VIEW_CDIC`
LIMIT
  0;


INSERT INTO
  `pcb-{env}-curated.cots_cdic.CDIC_0110`
SELECT
  *
FROM
  `pcb-{env}-processing.domain_cdic.MAIN_0110_VIEW_CDIC`;
