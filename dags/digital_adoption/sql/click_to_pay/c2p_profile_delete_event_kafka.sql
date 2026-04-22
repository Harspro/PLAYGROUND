EXPORT DATA
  OPTIONS (
    uri = 'gs://pcb-{env}-staging-extract/digital-adoption/c2p-profile-delete-event-*.parquet',
    format = 'PARQUET',
    OVERWRITE = TRUE)
AS (
  WITH
    CUSTOMER_BASE AS (
      SELECT *
      FROM
        `pcb-{env}-curated.domain_customer_management.CLICK_TO_PAY_EVENT_STATUS`
      QUALIFY
        ROW_NUMBER()
          OVER (PARTITION BY customerId ORDER BY INGESTION_TIMESTAMP DESC) = 1
        AND UPPER(requestType) != 'DELETE'
    ),
    INELIGIBLE_CUSTOMER AS (
      SELECT customerId
      FROM CUSTOMER_BASE
      WHERE
        emailAddress IS NOT NULL
        AND phoneNumber IS NOT NULL
      QUALIFY COUNT(*) OVER (PARTITION BY emailAddress, phoneNumber) > 1
    )
  SELECT
    CUSTOMER_BASE.customerId,
    CUSTOMER_BASE.accountId,
    CUSTOMER_BASE.firstName,
    CUSTOMER_BASE.lastName,
    CUSTOMER_BASE.emailAddress,
    CUSTOMER_BASE.phoneNumber,
    'CUSTOMER_UNENROLLED' AS flowType,
    'DELETE_OVERRIDE' AS requestType,
    CUSTOMER_BASE.languageCode,
    '' AS phoneCode,
    CUSTOMER_BASE.accessMediumUid,
    IF(
      COALESCE(CUSTOMER_BASE.addressLine1, '') != ''
        AND COALESCE(CUSTOMER_BASE.city, '') != ''
        AND COALESCE(CUSTOMER_BASE.state, '') != ''
        AND COALESCE(CUSTOMER_BASE.countryCode, '') != ''
        AND COALESCE(CUSTOMER_BASE.zip, '') != '',
      STRUCT(
        CUSTOMER_BASE.addressLine1 AS line1,
        NULLIF(CUSTOMER_BASE.addressLine2, '') AS line2,
        CUSTOMER_BASE.city AS city,
        CUSTOMER_BASE.state,
        CUSTOMER_BASE.countryCode,
        CUSTOMER_BASE.zip),
      NULL) AS address
  FROM CUSTOMER_BASE
  JOIN INELIGIBLE_CUSTOMER
    ON CUSTOMER_BASE.customerId = INELIGIBLE_CUSTOMER.customerId
);