INSERT INTO `pcb-{DEPLOY_ENV}-curated.domain_aml.AML_IDV_EXPERIAN_DATA`
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
    exp_acc_cust.CUSTOMER_IDENTIFIER_NO                             AS CUSTOMER_NUMBER,
    SAFE_CAST(exp_base.APP_NUMBER AS STRING)                        AS APP_NUM,
    exp_base.ID_TYPE                                                AS ID_TYPE,
    exp_base.ID_TYPE                                                AS ID_TYPE_DESCRIPTION,
    'PRIMARY'                                                       AS ID_STATUS,
    exp_base.ID_NUMBER                                              AS ID_NUMBER,
    exp_base.ID_STATE                                               AS ID_STATE,
    exp_base.ID_COUNTRY                                             AS ID_COUNTRY,
    SAFE_CAST(exp_base.ID_ISSUE_DATE AS STRING)                     AS ID_ISSUE_DATE,
    SAFE_CAST(exp_base.ID_EXPIRY_DATE AS STRING)                    AS ID_EXPIRY_DATE,
    exp_base.IDV_METHOD                                             AS IDV_METHOD,
    exp_base.IDV_DECISION                                           AS IDV_DECISION,
    CURRENT_DATETIME()                                              AS REC_CREATE_TMS,
    '{run_id}'                                                      AS RUN_ID
FROM
    `pcb-{DEPLOY_ENV}-processing.domain_aml.EXP_ACC_CUST` exp_acc_cust
    INNER JOIN `pcb-{DEPLOY_ENV}-processing.domain_aml.EXP_BASE` exp_base
        ON exp_acc_cust.CARD_NUMBER = exp_base.ACCESS_MEDIUM_NO