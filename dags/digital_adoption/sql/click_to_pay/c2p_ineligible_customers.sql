CREATE OR REPLACE TABLE
  pcb-{env}-curated.domain_account_management.C2P_INELIGIBLE_CUSTOMERS
AS
SELECT
  DISTINCT customer_uid,
  eligibility_flag,
  ineligibility_reason
FROM (
  SELECT
    *
  FROM
    pcb-{env}-processing.domain_account_management.C2P_NON_PRIMARY_ACCOUNTS
  UNION ALL
  SELECT
    *
  FROM
    pcb-{env}-processing.domain_account_management.C2P_BAD_STANDING_ACCOUNTS
  UNION ALL
  SELECT
    *
  FROM
    pcb-{env}-processing.domain_account_management.C2P_NO_EMAIL_ACCOUNTS
  UNION ALL
  SELECT
    *
  FROM
    pcb-{env}-processing.domain_account_management.C2P_BLOCK_CODES_ACCOUNT
  UNION ALL
  SELECT
    *
  FROM
    pcb-{env}-processing.domain_account_management.C2P_ABB_PCMA_ACCOUNTS);