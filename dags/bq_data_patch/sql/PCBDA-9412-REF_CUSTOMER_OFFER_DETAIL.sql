UPDATE
  pcb-{env}-landing.domain_account_management.REF_CUSTOMER_OFFER_DETAIL
SET
  offer_priority = 60
WHERE
  offer_type = 'Product Upgrade';

INSERT INTO
  pcb-{env}-landing.domain_account_management.REF_CUSTOMER_OFFER_DETAIL
VALUES
  (2,'Credit Limit Increase','CLI offer per TRIAD events',90,TRUE,DATETIME_TRUNC(CURRENT_DATETIME('America/Toronto'),SECOND),'IDL',DATETIME_TRUNC(CURRENT_DATETIME('America/Toronto'),SECOND));