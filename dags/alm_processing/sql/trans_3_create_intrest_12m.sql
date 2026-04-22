CREATE OR REPLACE VIEW `pcb-{env}-processing.cots_alm_tbsm.INTREST_12M` AS
SELECT 
    SUM(AMT_INT) AS INTREST_12M,
    MAST_ACCOUNT_ID
FROM
    pcb-{env}-processing.cots_alm_tbsm.AT31_AGG_TRANS
GROUP BY 
    MAST_ACCOUNT_ID