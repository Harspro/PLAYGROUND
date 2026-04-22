CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_customer_management.STMT_CME_FEEDBACK_DISTINCT
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) )
AS
SELECT
  DISTINCT account_id AS ACCOUNT_ID,
  customer_id AS CUSTOMER_ID,
  offer_id AS OFFER_ID
FROM
  pcb-{env}-landing.domain_marketing.STMT_FEEDBACK_STAGING
GROUP BY
  account_id,
  customer_id,
  offer_id