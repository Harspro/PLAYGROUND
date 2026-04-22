CREATE OR REPLACE VIEW `pcb-{env}-processing.cots_alm_tbsm.ASSET_CC_RECV_TRANS_10` AS
SELECT 
    TR9.*,
    CS.Description AS CREDIT_SCORE_ID,
FROM 
    pcb-{env}-processing.cots_alm_tbsm.ASSET_CC_RECV_TRANS_9 TR9
JOIN 
    pcb-{env}-landing.domain_treasury.ALM_CREDIT_SCORE_REF CS
ON 
    IFNULL(TR9.CIFP_CURRENT_CRED_SC, 0) BETWEEN CS.Min_Range AND CS.Max_Range