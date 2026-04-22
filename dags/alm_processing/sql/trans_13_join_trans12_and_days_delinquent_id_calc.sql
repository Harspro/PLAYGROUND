CREATE OR REPLACE VIEW `pcb-{env}-processing.cots_alm_tbsm.ASSET_CC_RECV_TRANS_13` AS
SELECT 
    TR12.*,
    CASE
    WHEN TR12.MAST_ACCOUNT_SUFFIX <> 0
    THEN {days_delinquent_id_1}
    WHEN DB.Description IS NOT NULL and TR12.CIFP_CURRENT_BALANCE <= 0
    THEN DB.Description
    WHEN DS.Decription IS NOT NULL
    THEN DS.Decription
    ELSE {days_delinquent_id_default}
    END AS DAYS_DELINQUENT_ID
FROM 
    pcb-{env}-processing.cots_alm_tbsm.ASSET_CC_RECV_TRANS_12 TR12
LEFT JOIN 
    pcb-{env}-landing.domain_treasury.ALM_DAYS_DELQ_AM02_BAL_CUR_REF DB 
ON 
    IFNULL(TR12.CIFP_CURRENT_BALANCE, 0) BETWEEN DB.Min_Range AND DB.Max_Range
LEFT JOIN 
    pcb-{env}-landing.domain_treasury.ALM_DAYS_DELQ_AM00_STATC_CUR_REF DS
ON 
    IFNULL(CAST(TR12.CIFP_CONS_DAYS_PASTDUE AS STRING), '') = DS.am00_statc_current_past_due