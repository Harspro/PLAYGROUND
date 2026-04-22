CREATE OR REPLACE VIEW `pcb-{env}-processing.domain_consent.cnst_events_analytics_v1_aopx_join` AS
SELECT
  v1.*,
  aopx.prod_id
FROM
  `pcb-{env}-processing.domain_consent.cnst_events_analytics_expb_cif_join` v1
LEFT JOIN
   `pcb-{env}-landing.domain_customer_acquisition.APP_OP_PROD_XREF` aopx 
ON
  v1.applicationid = aopx.pc_mc_cust_appl_id