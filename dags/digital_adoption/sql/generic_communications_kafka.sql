EXPORT DATA
  OPTIONS ( uri = 'gs://pcb-{env}-staging-extract/digital-adoption/generic-communications-*.parquet',
    format = 'PARQUET',
    OVERWRITE = TRUE ) AS (
  SELECT
    COMMUNICATION_CODE AS code,
    UNIQUE_ID AS uuid,
    SOURCE AS source,
    NULLIF(SOURCE_IDEMPOTENCY_KEY, '') AS sourceIdempotencyKey,
    NULLIF(LANGUAGE, '') AS language,
  IF
    ( NULLIF(CONTACT_EMAIL, '') IS NULL
      AND NULLIF(CONTACT_PHONE, '') IS NULL
      AND NULLIF(ALTERNATE_PHONE, '') IS NULL
      AND NULLIF(PUSH_TOKEN, '') IS NULL, NULL, STRUCT(
      IF
        (NULLIF(CONTACT_EMAIL, '') IS NOT NULL, STRUCT( CAST(CONTACT_EMAIL AS STRING) AS value,
            CAST(NULL AS BOOL) AS validated ), NULL ) AS email,
      IF
        (NULLIF(CONTACT_PHONE,'') IS NOT NULL, STRUCT( CAST(CONTACT_PHONE AS STRING) AS number,
            FALSE AS mobile ), NULL ) AS phone,
      IF
        (NULLIF(ALTERNATE_PHONE, '') IS NOT NULL, STRUCT( CAST(ALTERNATE_PHONE AS STRING) AS number,
            FALSE AS mobile ), NULL ) AS alternatePhone,
        CAST(NULLIF(PUSH_TOKEN, '') AS STRING) AS pushToken ) ) AS contact,
    SPLIT(NULLIF(CHANNELS, ''), ',') AS channels,
    NULLIF(DOMAIN_CODE, '') AS domainCode,
    NULLIF(TENANT, '') AS tenant,
    FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E6SZ', CURRENT_TIMESTAMP()) AS timestamp,
    STRUCT( CAST(PCF_CUSTOMER_ID AS STRING) AS pcfCustomerId,
      NULLIF(PRODUCT_CODE, '') AS productCode,
      NULLIF(ACCOUNT_ID, '') AS accountId,
      NULLIF(CUSTOMER_ID, '') AS customerId,
      CAST(NULL AS STRING) AS tsysAccountId,
      CAST(NULL AS STRING) AS tsysCustomerId,
      NULLIF(ACCOUNT_NUMBER, '') AS accountNumber,
      NULLIF(CUSTOMER_NUMBER, '') AS customerNumber,
      NULLIF(LAST_DIGITS, '') AS lastDigits,
      NULLIF(RECIPIENT_TIMEZONE, '') AS timezone,
      NULLIF(CUSTOMER_ROLE, '') AS accountCustomerRole ) AS identifiers,
    COALESCE( (
      SELECT
        ARRAY_AGG( STRUCT( JSON_VALUE(param, '$.paramName') AS name,
            CAST(JSON_VALUE(param, '$.paramValue') AS STRING) AS value,
            JSON_VALUE(param, '$.paramType') AS type ) )
      FROM
        UNNEST( JSON_EXTRACT_ARRAY(PARAMETERS) ) AS param ), [] ) AS parameters
  FROM
    `pcb-{env}-landing.domain_communication.GENERIC_BATCH_COMMUNICATION_REQUEST`
  WHERE
    SOURCE_FILENAME = '{file_name}' );