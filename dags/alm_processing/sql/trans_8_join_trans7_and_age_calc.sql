CREATE OR REPLACE VIEW `pcb-{env}-processing.cots_alm_tbsm.ASSET_CC_RECV_TRANS_8` AS
SELECT 
   TR7.*,
   AR.Description AS AGE
FROM 
   pcb-{env}-processing.cots_alm_tbsm.ASSET_CC_RECV_TRANS_7 TR7
JOIN
   pcb-{env}-landing.domain_treasury.ALM_AGE_OF_PORTFOLIO_REF AR 
ON 
   DATE_DIFF(CURRENT_DATE (), TR7.CIFP_DATE_OPEN, month) BETWEEN AR.Min_Range AND AR.Max_Range