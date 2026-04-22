EXPORT DATA
  OPTIONS (
    uri = 'gs://pcb-{env}-staging-extract/digital-adoption/c2p-back-book-*.parquet',
    format = 'PARQUET',
    OVERWRITE = TRUE)
AS (
  WITH
  LATEST_ACCESS_MEDIUM AS (
    SELECT account_customer_uid, access_medium_uid
    FROM pcb-{env}-curated.domain_account_management.ACCESS_MEDIUM
    QUALIFY
      ROW_NUMBER()
        OVER (PARTITION BY account_customer_uid ORDER BY update_dt DESC) = 1
  ),
  OPTED_OUT_CUSTOMERS AS (
    SELECT customerId
    FROM
      pcb-{env}-curated.domain_customer_management.CLICK_TO_PAY_CUSTOMER_STATUS
    QUALIFY
      ROW_NUMBER()
        OVER (PARTITION BY customerId ORDER BY ingestion_timestamp DESC) = 1
      AND optInStatus IS FALSE
  ),
  CONTACT AS (
    SELECT
      customer_uid,
      MAX(IF(UPPER(type) = 'PHONE', contact_uid, NULL)) AS phone_contact_uid,
      MAX(IF(UPPER(type) = 'EMAIL', contact_uid, NULL)) AS email_contact_uid,
      MAX(IF(UPPER(type) = 'ADDRESS', contact_uid, NULL)) AS address_contact_uid
    FROM pcb-{env}-curated.domain_customer_management.CONTACT
    WHERE
      UPPER(context) = 'PRIMARY'
      AND UPPER(type) IN ('PHONE', 'EMAIL', 'ADDRESS')
    GROUP BY customer_uid
  ),
  CUST_DETAILS as(
  SELECT
      CONTACT.customer_uid,
      email_address,
      CONCAT(area_code, phone_number) phone_number
    FROM CONTACT
    JOIN pcb-{env}-curated.domain_account_management.C2P_ELIGIBLE_CUSTOMERS
      ON CONTACT.customer_uid = C2P_ELIGIBLE_CUSTOMERS.customer_uid
    LEFT JOIN pcb-{env}-curated.domain_customer_management.EMAIL_CONTACT
      ON CONTACT.email_contact_uid = EMAIL_CONTACT.contact_uid
    LEFT JOIN pcb-{env}-curated.domain_customer_management.PHONE_CONTACT
      ON CONTACT.phone_contact_uid = PHONE_CONTACT.contact_uid
  ),
  DUPLICATE_VALUES AS (
    SELECT
      email_address,
      phone_number
    FROM CUST_DETAILS
    GROUP BY email_address, phone_number
    HAVING COUNT(*) > 1
  )
SELECT
  CAST(CUSTOMER.customer_uid AS STRING) AS customerId,
  NULLIF(CAST(ACTIVE_CUSTOMER_BASE.account_uid AS STRING), '') AS accountId,
  CUSTOMER.given_name AS firstName,
  CUSTOMER.surname AS lastName,
  NULLIF(EMAIL_CONTACT.email_address, '') AS emailAddress,
  NULLIF(CAST(CONCAT(PHONE_CONTACT.area_code,PHONE_CONTACT.phone_number) AS STRING), '') AS phoneNumber,
  'BACK_BOOK_LOADING' AS flowType,
  'ENROLL' AS requestType,
  NULLIF(UPPER(CUSTOMER_PREFERENCE_PARAM.value), '') AS languageCode,
  NULLIF(CAST(PHONE_CONTACT.country_code AS STRING), '') AS phoneCode,
  NULLIF(CAST(LATEST_ACCESS_MEDIUM.access_medium_uid AS STRING), '') AS accessMediumUid,
  IF(
    COALESCE(ADDRESS_CONTACT.address_line_1, '') != ''
      AND COALESCE(ADDRESS_CONTACT.city, '') != ''
      AND COALESCE(ADDRESS_CONTACT.province_state, '') != ''
      AND COALESCE(ADDRESS_CONTACT.country, '') != ''
      AND COALESCE(ADDRESS_CONTACT.postal_zip_code, '') != '',
    STRUCT(
      ADDRESS_CONTACT.address_line_1 AS line1,
      NULLIF(ADDRESS_CONTACT.address_line_2, '') AS line2,
      ADDRESS_CONTACT.city AS city,
      ADDRESS_CONTACT.province_state AS state,
      ADDRESS_CONTACT.country AS countryCode,
      ADDRESS_CONTACT.postal_zip_code AS zip),
    NULL) AS address
FROM pcb-{env}-curated.domain_account_management.C2P_ELIGIBLE_CUSTOMERS
JOIN
  pcb-{env}-processing.domain_account_management.ACTIVE_CUSTOMER_BASE
  ON C2P_ELIGIBLE_CUSTOMERS.customer_uid = ACTIVE_CUSTOMER_BASE.customer_uid
JOIN
  pcb-{env}-curated.domain_customer_management.CUSTOMER
  ON C2P_ELIGIBLE_CUSTOMERS.customer_uid = CUSTOMER.customer_uid
JOIN
  CONTACT
  ON CUSTOMER.customer_uid = CONTACT.customer_uid
LEFT JOIN
  pcb-{env}-curated.domain_customer_management.EMAIL_CONTACT
  ON CONTACT.email_contact_uid = EMAIL_CONTACT.contact_uid
LEFT JOIN
  pcb-{env}-curated.domain_customer_management.PHONE_CONTACT
  ON CONTACT.phone_contact_uid = PHONE_CONTACT.contact_uid
LEFT JOIN
  pcb-{env}-curated.domain_customer_management.ADDRESS_CONTACT
  ON CONTACT.address_contact_uid = ADDRESS_CONTACT.contact_uid
LEFT JOIN
  LATEST_ACCESS_MEDIUM
  ON ACTIVE_CUSTOMER_BASE.account_customer_uid = LATEST_ACCESS_MEDIUM.account_customer_uid
LEFT JOIN
  pcb-{env}-curated.domain_customer_management.CUSTOMER_PREFERENCE
  ON CUSTOMER.customer_uid = CUSTOMER_PREFERENCE.customer_uid
  AND UPPER(CUSTOMER_PREFERENCE.code) = 'LANGUAGE'
LEFT JOIN
  pcb-{env}-curated.domain_customer_management.CUSTOMER_PREFERENCE_PARAM
  ON CUSTOMER_PREFERENCE.customer_preference_uid = CUSTOMER_PREFERENCE_PARAM.customer_preference_uid
LEFT JOIN
  OPTED_OUT_CUSTOMERS
  ON CUSTOMER.customer_uid = OPTED_OUT_CUSTOMERS.customerId
LEFT JOIN
  pcb-{env}-processing.domain_account_management.CLOSED_BLOCK_CODES
  ON CUSTOMER.customer_uid = CLOSED_BLOCK_CODES.customer_uid
  AND ACTIVE_CUSTOMER_BASE.account_no = CLOSED_BLOCK_CODES.cifp_account_id5
LEFT JOIN
  DUPLICATE_VALUES
  ON EMAIL_CONTACT.email_address = DUPLICATE_VALUES.email_address
  AND CONCAT(PHONE_CONTACT.area_code,PHONE_CONTACT.phone_number) = DUPLICATE_VALUES.phone_number
WHERE
  OPTED_OUT_CUSTOMERS.customerId IS NULL
  AND CLOSED_BLOCK_CODES.cifp_account_id5 IS NULL
  AND DUPLICATE_VALUES.email_address IS NULL
);
