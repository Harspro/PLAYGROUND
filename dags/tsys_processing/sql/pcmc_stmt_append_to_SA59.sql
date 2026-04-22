SELECT
  *
FROM
  pcb-{env}-processing.domain_account_management.SA59_DAILY{file_type}
UNION ALL
SELECT
  *
FROM
  pcb-{env}-processing.domain_account_management.SA59_CALC{file_type}