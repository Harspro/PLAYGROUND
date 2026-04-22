CREATE OR REPLACE VIEW `pcb-{env}-processing.cots_alm_tbsm.ASSET_CC_RECV_TRANS_11` AS
SELECT 
    TR10.*,
    AB.Description AS BALANCE,
FROM 
    pcb-{env}-processing.cots_alm_tbsm.ASSET_CC_RECV_TRANS_10 TR10
JOIN 
    pcb-{env}-landing.domain_treasury.ALM_ACCOUNT_BALANCE_REF AB 
ON 
    IFNULL(TR10.CIFP_CURRENT_BALANCE, 0) BETWEEN AB.Min_Range AND AB.Max_Range