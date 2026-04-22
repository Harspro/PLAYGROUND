CREATE OR REPLACE VIEW `pcb-{env}-processing.cots_alm_tbsm.ASSET_CC_RECV_TRANS_12` AS
SELECT 
    TR11.*,
    CL.Description AS CREDIT_LIMIT
FROM 
    pcb-{env}-processing.cots_alm_tbsm.ASSET_CC_RECV_TRANS_11 TR11
JOIN 
    pcb-{env}-landing.domain_treasury.ALM_CREDIT_LIMIT_REF CL 
ON 
    IFNULL(TR11.CIFP_CREDIT_LIMIT, 0) BETWEEN CL.Min_Range AND CL.Max_Range