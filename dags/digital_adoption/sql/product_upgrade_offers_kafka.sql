EXPORT DATA
  OPTIONS (
    uri = 'gs://pcb-{env}-staging-extract/product_upgrade_offers/product_upgrade_offers-*.parquet',
    format = 'PARQUET',
    overwrite = true
  )
AS
(
SELECT
    CAST(OFFER_ID AS STRING) AS OFFER_ID,
    CAST(OFFER_PRIORITY AS STRING) AS OFFER_PRIORITY,
    CAST(ACCOUNT_ID AS STRING) AS ACCOUNT_ID,
    CAST(CUSTOMER_ID AS STRING) AS CUSTOMER_ID,
    CURRENT_VALUE,
    ELIGIBLE_VALUE,
    CAST(OFFER_CREATED_DT AS STRING) AS OFFER_CREATED_DT
FROM
  `pcb-{env}-landing.domain_account_management.PRODUCT_UPGRADE_OFFER`
WHERE
  DATE(REC_LOAD_TIMESTAMP) = (SELECT MAX(DATE(REC_LOAD_TIMESTAMP)) FROM `pcb-{env}-landing.domain_account_management.PRODUCT_UPGRADE_OFFER`)
);
