CREATE OR REPLACE TABLE
  pcb-{env}-curated.domain_account_management.C2P_ELIGIBLE_CUSTOMERS AS
SELECT
  PERMITTED_CUSTOMERS.customer_uid,
  CASE
    WHEN CUSTOMER.PLATFORM_USER_UID IS NULL THEN 'N'
    ELSE 'Y'
END
  AS dp_enrolled_ind,
  ROW_NUMBER() OVER (ORDER BY PERMITTED_CUSTOMERS.customer_uid) AS seq_no,
  CURRENT_DATETIME('America/Toronto') AS rec_load_timestamp
FROM
  pcb-{env}-processing.domain_account_management.PERMITTED_CUSTOMERS
LEFT JOIN
  pcb-{env}-curated.domain_customer_management.CUSTOMER
ON
  PERMITTED_CUSTOMERS.customer_uid = CUSTOMER.customer_uid;