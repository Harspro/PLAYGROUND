CREATE OR REPLACE TABLE `pcb-{DEPLOY_ENV}-processing.domain_aml.EXP_ACC_CUST`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
)
AS
SELECT
  customer_identifier.CUSTOMER_IDENTIFIER_NO,
  access_medium.CARD_NUMBER
FROM
  `pcb-{DEPLOY_ENV}-curated.domain_account_management.ACCESS_MEDIUM` access_medium
  INNER JOIN `pcb-{DEPLOY_ENV}-curated.domain_account_management.ACCOUNT_CUSTOMER` account_customer
    ON access_medium.ACCOUNT_CUSTOMER_UID = account_customer.ACCOUNT_CUSTOMER_UID
  INNER JOIN `pcb-{DEPLOY_ENV}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` customer_identifier
    ON account_customer.CUSTOMER_UID = customer_identifier.CUSTOMER_UID
    AND customer_identifier.TYPE='PCF-CUSTOMER-ID'
    AND customer_identifier.DISABLED_IND='N'
    AND account_customer.ACCOUNT_CUSTOMER_ROLE_UID = 1