CREATE OR REPLACE VIEW `pcb-{env}-processing.domain_consent.cnst_events_analytics_expb_cif_join` AS
SELECT 
  expb.*,
  cif.cifp_pcf_cust_id AS pcfcustid,
  cif.cifp_account_num
FROM 
  `pcb-{env}-processing.domain_consent.cnst_events_analytics_expb_unpvt_expb_join` expb
LEFT JOIN
  `pcb-{env}-curated.domain_account_management.CIF_CARD_CURR` cif
ON 
  expb.mccard_number_fdr = cif.cifp_account_num