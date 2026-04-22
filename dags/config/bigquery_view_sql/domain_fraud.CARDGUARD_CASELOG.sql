SELECT * 
FROM `pcb-{env}-landing.domain_fraud.CARDGUARD_CASELOG` 
WHERE FILE_CREATE_DT IN (
    SELECT MAX(FILE_CREATE_DT) 
    FROM `pcb-{env}-landing.domain_fraud.CARDGUARD_CASELOG_TRLR`
    )