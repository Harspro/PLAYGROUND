CREATE OR REPLACE VIEW `pcb-{env}-processing.domain_consent.cnst_events_analytics_v3_reg_join` AS
SELECT
  v3.*,
  reg.app_type,
  reg.source_cd
FROM
  `pcb-{env}-processing.domain_consent.cnst_events_analytics_v2_aop_join` v3
LEFT JOIN
  `pcb-{env}-processing.domain_consent.pcmc_reg_union_exp_pcmc_reg` reg 
ON
  v3.applicationid = reg.pc_mc_cust_appl_id