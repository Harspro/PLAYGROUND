INSERT INTO `pcb-{DEPLOY_ENV}-curated.domain_aml.CUST_IDV_INFO`
(
    CUSTOMER_NUMBER,
    ID_TYPE,
    ID_TYPE_DESCRIPTION,
    ID_STATUS,
    ID_NUMBER,
    ID_STATE,
    ID_COUNTRY,
    ID_ISSUE_DATE,
    ID_EXPIRY_DATE,
    NAME_ON_SOURCE,
    REF_NUMBER,
    DATE_VERIFIED,
    TYPE_OF_INFO,
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
    ID_ISSUE_DATE,
    ID_EXPIRY_DATE,
    NAME_ON_SOURCE,
    REF_NUMBER,
    DATE_VERIFIED,
    TYPE_OF_INFO,
    IDV_METHOD,
    IDV_DECISION,
    CREATE_DT
FROM
(
    SELECT
        aml_idv_experian_data.CUSTOMER_NUMBER                                               AS CUSTOMER_NUMBER,
        aml_idv_experian_data.ID_TYPE                                                       AS ID_TYPE,
        aml_idv_experian_data.ID_TYPE_DESCRIPTION                                           AS ID_TYPE_DESCRIPTION,
        aml_idv_experian_data.ID_STATUS                                                     AS ID_STATUS,
        aml_idv_experian_data.ID_NUMBER                                                     AS ID_NUMBER,
        aml_idv_experian_data.ID_STATE                                                      AS ID_STATE,
        aml_idv_experian_data.ID_COUNTRY                                                    AS ID_COUNTRY,
        SAFE.PARSE_DATE('%m%d%Y',LPAD(aml_idv_experian_data.ID_ISSUE_DATE , 8 , '0'))       AS ID_ISSUE_DATE,
        SAFE.PARSE_DATE('%m%d%Y',LPAD(aml_idv_experian_data.ID_EXPIRY_DATE , 8 , '0'))      AS ID_EXPIRY_DATE,
        SAFE_CAST(NULL AS STRING)                                                           AS NAME_ON_SOURCE,
        SAFE_CAST(NULL AS STRING)                                                           AS REF_NUMBER,
        SAFE_CAST(NULL AS DATE)                                                             AS DATE_VERIFIED,
        SAFE_CAST(NULL AS STRING)                                                           AS TYPE_OF_INFO,
        aml_idv_experian_data.IDV_METHOD                                                    AS IDV_METHOD,
        aml_idv_experian_data.IDV_DECISION                                                  AS IDV_DECISION,
        CURRENT_DATETIME()                                                                  AS CREATE_DT,
        ROW_NUMBER() OVER (PARTITION BY aml_idv_experian_data.CUSTOMER_NUMBER 
                            ORDER BY aml_idv_experian_data.APP_NUM DESC)                    AS REC_RANK
    FROM
        `pcb-{DEPLOY_ENV}-curated.domain_aml.AML_IDV_EXPERIAN_DATA`  AS aml_idv_experian_data
    WHERE
        NOT EXISTS
        (
            SELECT
                cust_idv.CUSTOMER_NUMBER
            FROM
                `pcb-{DEPLOY_ENV}-curated.domain_aml.CUST_IDV_INFO` cust_idv
            WHERE
                cust_idv.CUSTOMER_NUMBER = aml_idv_experian_data.CUSTOMER_NUMBER
        )
        AND EXISTS
        (
            SELECT
                customer_feed.CUSTOMER_NUMBER
            FROM
                `pcb-{DEPLOY_ENV}-curated.cots_aml_verafin.CUSTOMER_FEED` customer_feed
            WHERE
                customer_feed.CUSTOMER_NUMBER = aml_idv_experian_data.CUSTOMER_NUMBER
        )
)
WHERE
    REC_RANK = 1