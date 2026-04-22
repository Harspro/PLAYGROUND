INSERT INTO `pcb-{DEPLOY_ENV}-curated.domain_aml.CUST_IDV_INFO`
(
    CUSTOMER_NUMBER,
    ID_TYPE,
    ID_TYPE_DESCRIPTION,
    ID_STATUS,
    ID_NUMBER,
    ID_STATE,
    ID_COUNTRY,
    IDV_METHOD,
    IDV_DECISION,
    CREATE_DT
)
SELECT
    CUSTOMER_NUMBER,
    ID_TYPE,
    ID_TYPE_DESCRIPTION,
    ID_STATUS,
    ID_NUMBER,
    ID_STATE,
    ID_COUNTRY,
    IDV_METHOD,
    IDV_DECISION,
    CREATE_DT
FROM
(
    SELECT
        aml_idv_cp_data.CUSTOMER_NUMBER                                               AS CUSTOMER_NUMBER,
        aml_idv_cp_data.ID_TYPE                                                       AS ID_TYPE,
        aml_idv_cp_data.ID_TYPE                                                       AS ID_TYPE_DESCRIPTION,
        aml_idv_cp_data.ID_STATUS                                                     AS ID_STATUS,
        aml_idv_cp_data.ID_NUMBER                                                     AS ID_NUMBER,
        aml_idv_cp_data.ID_STATE                                                      AS ID_STATE,
        aml_idv_cp_data.ID_COUNTRY                                                    AS ID_COUNTRY,
        SAFE_CAST(NULL AS STRING)                                                     AS ID_ISSUE_DATE,
        SAFE_CAST(NULL AS STRING)                                                     AS ID_EXPIRY_DATE,
        aml_idv_cp_data.IDV_METHOD                                                    AS IDV_METHOD,
        aml_idv_cp_data.IDV_DECISION                                                  AS IDV_DECISION,
        CURRENT_DATETIME()                                                            AS CREATE_DT,
        ROW_NUMBER() OVER (PARTITION BY aml_idv_cp_data.CUSTOMER_NUMBER
                            ORDER BY aml_idv_cp_data.APP_NUM DESC)                    AS REC_RANK
    FROM
        `pcb-{DEPLOY_ENV}-curated.domain_aml.AML_IDV_CANADAPOST_DATA`  AS aml_idv_cp_data
    WHERE
        NOT EXISTS
        (
            SELECT
                cust_idv.CUSTOMER_NUMBER
            FROM
                `pcb-{DEPLOY_ENV}-curated.domain_aml.CUST_IDV_INFO` cust_idv
            WHERE
                cust_idv.CUSTOMER_NUMBER = aml_idv_cp_data.CUSTOMER_NUMBER
        )
        AND EXISTS
        (
            SELECT
                customer_feed.CUSTOMER_NUMBER
            FROM
                `pcb-{DEPLOY_ENV}-curated.cots_aml_verafin.CUSTOMER_FEED` customer_feed
            WHERE
                customer_feed.CUSTOMER_NUMBER = aml_idv_cp_data.CUSTOMER_NUMBER
        )
)
WHERE
    REC_RANK = 1