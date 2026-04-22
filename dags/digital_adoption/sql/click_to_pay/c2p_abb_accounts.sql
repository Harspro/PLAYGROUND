CREATE OR REPLACE VIEW
  pcb-{env}-processing.domain_account_management.ABB_ACCOUNTS
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS
SELECT
  DISTINCT CAST(LTTR.ts2_account_id AS INT64) AS account_number
FROM
  pcb-{env}-processing.domain_account_management.NEW_APPLICATION
INNER JOIN
  pcb-{env}-processing.domain_account_management.ABB_APPLICATION
ON
  NEW_APPLICATION.apa_app_num = ABB_APPLICATION.apa_app_num
  AND NEW_APPLICATION.max_execution_id = ABB_APPLICATION.execution_id
INNER JOIN
  `pcb-{env}-curated.domain_customer_acquisition.LTTR_OVRD_DETAIL_XTO` LTTR
ON
  LTTR.app_num = NEW_APPLICATION.apa_app_num
  AND LTTR.ts2_account_id IS NOT NULL
  AND LTTR.ts2_account_id <> 0
INNER JOIN
  `pcb-{env}-curated.domain_account_management.CIF_ACCOUNT_CURR` CIF
ON
  CAST(LTTR.ts2_account_id AS INT64) = CAST(CIF.cifp_account_id5 AS INT64)
WHERE
  CIF.cifp_closed_maint_date IS NULL;