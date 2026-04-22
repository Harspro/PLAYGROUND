CREATE OR REPLACE VIEW `pcb-{env}-processing.cots_alm_tbsm.ASSET_CC_RECV_TRANS_5` AS
SELECT 
    TR4.*,
    AM00.MAST_ACCOUNT_SUFFIX,
    AM00.MAST_ACCOUNT_ID,
    AM00.CLIENT_PRODUCT_CODE,
    CASE
    WHEN AM00.AM00_SECURITIZATION_POOL_ID1 IN ({am00_securitization_pool_id1_val})
    THEN {securitized_val_1}
    ELSE {securitized_val_2}
    END AS SECURITIZED
FROM 
    pcb-{env}-processing.cots_alm_tbsm.ASSET_CC_RECV_TRANS_4 TR4
JOIN 
    pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM00_REC AM00
ON 
    TR4.CIFP_ACCOUNT_ID5 = SAFE_CAST(AM00.MAST_ACCOUNT_ID AS NUMERIC)
WHERE
    AM00.MAST_ACCOUNT_SUFFIX = {mast_account_suffix_val}