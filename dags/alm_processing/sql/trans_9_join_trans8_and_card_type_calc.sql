CREATE OR REPLACE VIEW `pcb-{env}-processing.cots_alm_tbsm.ASSET_CC_RECV_TRANS_9` AS
SELECT 
    TR8.*, 
    CASE
    WHEN ACT.Prd_Cd IS NOT NULL
    THEN UPPER(ACT.Description)
    ELSE {card_type_default}
    END AS CARD_TYPE
FROM 
    pcb-{env}-processing.cots_alm_tbsm.ASSET_CC_RECV_TRANS_8 TR8
LEFT JOIN 
    pcb-{env}-landing.domain_treasury.ALM_CARD_TYPE_REF ACT 
ON 
    TR8.CLIENT_PRODUCT_CODE = ACT.Prd_Cd