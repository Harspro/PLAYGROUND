CREATE OR REPLACE TABLE `pcb-{env}-curated.cots_alm_tbsm.OFF_BAL_SHEET_BOND_FORWARD_STG` AS
WITH split_data as (
SELECT 
    SPLIT(ALL_RECORDS,'|') as parts
FROM
    pcb-{env}-landing.cots_alm_tbsm.OFF_BAL_SHEET_BOND_FORWARD_TMP
WHERE 
    ALL_RECORDS NOT like '%HEADER%' 
    AND ALL_RECORDS NOT like 'TRAILER%')
SELECT 
    parts[OFFSET(0)] AS row_type,
    parts[OFFSET(1)] AS trade_id,
    parts[OFFSET(2)] AS counterparty,
    parts[OFFSET(3)] AS bond_cusip,
    parts[OFFSET(4)] AS trade_date,
    parts[OFFSET(5)] AS settlement_date,
    parts[OFFSET(6)] AS forward_termination_date,
    parts[OFFSET(7)] AS forward_settlement_date,
    parts[OFFSET(8)] AS contract_forward_price,
    parts[OFFSET(9)] AS face_amount
FROM
    split_data
