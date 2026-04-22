CREATE OR REPLACE TABLE
  pcb-{env}-curated.domain_account_management.C2P_CUSTOMER_ELIGIBILITY AS
SELECT
  DISTINCT customer_uid,
  eligibility_flag,
  ineligibility_reason,
  CURRENT_DATETIME('America/Toronto') AS rec_load_timestamp
FROM
  pcb-{env}-processing.domain_account_management.UNIFIED_CUSTOMER_ELIGIBILITY
WHERE
  {report_generation_flag} IS TRUE;