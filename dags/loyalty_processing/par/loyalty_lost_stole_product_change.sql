EXPORT DATA
  OPTIONS (
    uri = 'gs://pcb-{env}-staging-extract/sync-loyalty-profile/lost_stole_product_change-*.parquet',
    format = 'PARQUET',
    overwrite = true
  )
AS
(
SELECT
  DISTINCT SAFE_CAST(t3.CUSTOMER_IDENTIFIER_NO AS STRING) AS CUSTOMER_IDENTIFIER_NO,
  SAFE_CAST(t3.CUSTOMER_UID AS STRING) AS CUSTOMER_UID,
  SAFE_CAST('SYNC' AS STRING) AS SYNC_LOYALTY_EVENT_TYPE
FROM
  `pcb-{env}-landing.domain_account_management.ACCESS_MEDIUM` t1
JOIN
  `pcb-{env}-landing.domain_account_management.ACCOUNT_CUSTOMER` t2
ON
  t1.ACCOUNT_CUSTOMER_UID = t2.ACCOUNT_CUSTOMER_UID
JOIN
  `pcb-{env}-landing.domain_customer_management.CUSTOMER_IDENTIFIER` t3
ON
  t2.CUSTOMER_UID = t3.CUSTOMER_UID
WHERE
  LOWER(t1.ACCOUNT_RELATIONSHIP_STATUS) IN ('u',
    't')
  AND t1.CREATE_DT > '2024-09-01'
  AND LOWER(TYPE) = 'pcf-customer-id'
);