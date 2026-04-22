INSERT INTO `pcb-{DEPLOY_ENV}-curated.domain_aml.AML_IDV_CANADAPOST_DATA`
(
    CUSTOMER_NUMBER,
    APP_NUM,
    ID_TYPE,
    ID_TYPE_DESCRIPTION,
    ID_STATUS,
    ID_NUMBER,
    ID_STATE,
    ID_COUNTRY,
    ID_ISSUE_DATE,
    ID_EXPIRY_DATE,
    IDV_METHOD,
    IDV_DECISION,
    REC_CREATE_TMS,
    RUN_ID
)
SELECT
    cp_acc_cust.CUSTOMER_IDENTIFIER_NO                             AS CUSTOMER_NUMBER,
    SAFE_CAST(cp_base.APP_NUMBER AS STRING)                        AS APP_NUM,
    cp_base.ID_TYPE                                                AS ID_TYPE,
    cp_base.ID_TYPE                                                AS ID_TYPE_DESCRIPTION,
    'PRIMARY'                                                      AS ID_STATUS,
    cp_base.ID_NUMBER                                              AS ID_NUMBER,
    cp_base.ID_STATE                                               AS ID_STATE,
    cp_base.ID_COUNTRY                                             AS ID_COUNTRY,
    SAFE_CAST(NULL AS STRING)                                      AS ID_ISSUE_DATE,
    SAFE_CAST(NULL AS STRING)                                      AS ID_EXPIRY_DATE,
    cp_base.IDV_METHOD                                             AS IDV_METHOD,
    cp_base.IDV_DECISION                                           AS IDV_DECISION,
    CURRENT_DATETIME()                                             AS REC_CREATE_TMS,
    '{run_id}'                                                     AS RUN_ID
FROM
    `pcb-{DEPLOY_ENV}-processing.domain_aml.CANADAPOST_ACC_CUST` cp_acc_cust
    INNER JOIN `pcb-{DEPLOY_ENV}-processing.domain_aml.CANADAPOST_BASE` cp_base
        ON cp_acc_cust.ACCOUNT_NUMBER = cp_base.ACCOUNT_NUMBER