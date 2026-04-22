INSERT INTO `pcb-{DEPLOY_ENV}-curated.domain_aml.CUST_IDV_INFO` (CUSTOMER_NUMBER, ID_TYPE, ID_TYPE_DESCRIPTION, CREATE_DT)
        SELECT DISTINCT
        CUSTOMER_NUMBER,
        'ABB' as ID_TYPE,
        'Access to Basic Banking' as ID_TYPE_DESCRIPTION,
        CURRENT_DATETIME() as CREATE_DT
        FROM
        `pcb-{DEPLOY_ENV}-curated.domain_aml.AML_ABB_CUST`
        WHERE CUSTOMER_NUMBER NOT IN (
        SELECT CUSTOMER_NUMBER FROM `pcb-{DEPLOY_ENV}-curated.domain_aml.CUST_IDV_INFO`);