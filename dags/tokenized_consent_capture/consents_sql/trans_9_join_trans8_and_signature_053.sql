CREATE OR REPLACE VIEW `pcb-{env}-processing.domain_consent.cnst_events_analytics_v5_sig_join` AS
SELECT
  v5.*,
  sig.img_path AS singaturepathoriginal,
  SUBSTR(sig.img_path, INSTR(sig.img_path,'/',-1)+1, LENGTH(sig.img_path) - INSTR(sig.img_path,'/',-1)) signaturefile,
  IF(sig.IMG_PATH IS NOT NULL, 
      CONCAT("/Consent/",FORMAT_DATE('%Y/%m', sig.IMG_RECIEVED_DATE),"/",
      SUBSTR(sig.IMG_PATH,INSTR(sig.IMG_PATH, '/',-1)+ 1,LENGTH(sig.IMG_PATH) - INSTR(sig.IMG_PATH, '/',-1))), 
        NULL) AS signaturepathcurrent
FROM
  `pcb-{env}-processing.domain_consent.cnst_events_analytics_v4_adj_join` v5
LEFT JOIN
  `pcb-{env}-landing.domain_customer_acquisition.PCMC_SIGNATURE` sig
ON
  v5.applicationid = sig.pc_mc_cust_appl_id