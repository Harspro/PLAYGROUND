CREATE OR REPLACE VIEW
  pcb-{env}-processing.domain_account_management.ABB_APPLICATION
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS
SELECT
  DISTINCT apa_app_num,
  execution_id,
  dtc_bxr_source_1 AS promo_code
FROM
  `pcb-{env}-curated.domain_customer_acquisition.ADM_STD_DISP_BAL_TRNS_INFO`
WHERE
  dtc_bxr_source_1 = '00011535';