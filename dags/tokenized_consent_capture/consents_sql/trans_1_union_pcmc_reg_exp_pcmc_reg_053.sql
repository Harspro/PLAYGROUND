CREATE OR REPLACE VIEW `pcb-{env}-processing.domain_consent.pcmc_reg_union_exp_pcmc_reg` AS
SELECT 
    reg.pc_mc_cust_appl_id,
    reg.source_cd,
    reg.app_type
FROM 
    `pcb-{env}-landing.domain_customer_acquisition.PCMC_REGISTRATION` reg
UNION ALL
SELECT
    exp_reg.pc_mc_cust_appl_id,
    exp_reg.source_cd,
    exp_reg.app_type
FROM 
    `pcb-{env}-landing.domain_customer_acquisition.EXPERIAN_PCMC_REGISTRATION` exp_reg