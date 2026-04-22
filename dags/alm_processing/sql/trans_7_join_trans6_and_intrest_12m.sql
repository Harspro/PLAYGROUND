CREATE OR REPLACE VIEW `pcb-{env}-processing.cots_alm_tbsm.ASSET_CC_RECV_TRANS_7` AS
SELECT 
    TR6.*,
    INTR.INTREST_12M AS CUM_INTR_12M,
    CASE
    WHEN INTR.INTREST_12M >= {intrest_12m_val}
    THEN {account_type_1}
    ELSE {account_type_2}
    END AS ACCOUNT_TYPE
FROM 
    pcb-{env}-processing.cots_alm_tbsm.ASSET_CC_RECV_TRANS_6 TR6
LEFT JOIN 
    pcb-{env}-processing.cots_alm_tbsm.INTREST_12M INTR 
ON 
    TR6.CIFP_ACCOUNT_ID5 = INTR.MAST_ACCOUNT_ID