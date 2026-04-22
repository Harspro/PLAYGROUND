CREATE OR REPLACE VIEW `pcb-{env}-processing.domain_consent.cnst_events_analytics_expb_unpvt_expb_join` AS
SELECT
  DATETIME(expb.p_pi_signature_date) AS eventdate,
  DATETIME(expb.app_entered_date) AS sourcedt,
  expb.store AS sourceorg,
  expb.language_code AS cnstlangcode,
  expb.loblawappid AS applicationid,
  expb.mccard_number_fdr,
  unpv.cnstcategory
FROM 
  `pcb-{env}-landing.domain_customer_acquisition.EXPERIAN_BASE` expb
JOIN
  `pcb-{env}-processing.domain_consent.cnst_events_analytics_expb_ind_unpivot` unpv
ON 
  expb.loblawappid = unpv.applicationid