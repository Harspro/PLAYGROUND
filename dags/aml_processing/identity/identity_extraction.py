import logging
from util.miscutils import read_variable_or_file

DEPLOY_ENV = read_variable_or_file('gcp_config')['deployment_environment_name']

IDENTITY_TABLE = f"pcb-{DEPLOY_ENV}-curated.domain_aml.CUST_IDV_INFO"


def get_identity_feed_extract_sql() -> str:
    id_type_description = """CASE
            WHEN IDV_METHOD IN ('FTF', 'PAV', 'DIDV', 'FTFP', 'NFTF', 'FTFT') OR IDV_METHOD IS NULL
                THEN NULLIF(ID_TYPE_DESCRIPTION, '')
            WHEN IDV_METHOD = 'Credit File - Single Source'
                THEN NULLIF(NAME_ON_SOURCE, '')
            WHEN IDV_METHOD = 'Credit File - Dual Source' OR IDV_METHOD = 'Dual Source'
                THEN NULLIF(NAME_ON_SOURCE, '')
            END
            """

    id_number = """CASE
                WHEN IDV_METHOD IN ('FTF', 'PAV', 'DIDV', 'FTFP', 'NFTF', 'FTFT') OR IDV_METHOD IS NULL
                    THEN NULLIF(ID_Number, '')
                WHEN IDV_METHOD = 'Credit File - Single Source'
                    THEN NULLIF(REF_NUMBER, '')
                WHEN IDV_METHOD = 'Credit File - Dual Source' OR IDV_METHOD = 'Dual Source'
                    THEN NULLIF(REF_NUMBER, '')
                END
                """

    id_state = """CASE
                WHEN IDV_METHOD IN ('FTF', 'PAV', 'DIDV', 'FTFP', 'NFTF', 'FTFT') OR IDV_METHOD IS NULL
                    THEN NULLIF(ID_STATE, '')
                WHEN IDV_METHOD = 'Credit File - Single Source'
                    THEN NULL
                WHEN IDV_METHOD = 'Credit File - Dual Source' OR IDV_METHOD = 'Dual Source'
                    THEN NULL
                END
                """

    id_country = """CASE
                WHEN IDV_METHOD IN ('FTF', 'PAV', 'DIDV', 'FTFP', 'NFTF', 'FTFT') OR IDV_METHOD IS NULL
                    THEN NULLIF(ID_COUNTRY, '')
                WHEN IDV_METHOD = 'Credit File - Single Source'
                    THEN 'CA'
                WHEN IDV_METHOD = 'Credit File - Dual Source' OR IDV_METHOD = 'Dual Source'
                    THEN 'CA'
                END
                """

    id_issue_date = """CASE
                WHEN IDV_METHOD IN ('FTF', 'PAV', 'DIDV', 'FTFP', 'NFTF', 'FTFT') OR IDV_METHOD IS NULL
                    THEN DATE(ID_ISSUE_DATE)
                WHEN IDV_METHOD = 'Credit File - Single Source'
                    THEN DATE_VERIFIED
                WHEN IDV_METHOD = 'Credit File - Dual Source' OR IDV_METHOD = 'Dual Source'
                    THEN DATE_VERIFIED
                END
                """

    id_expiry_date = """CASE
                    WHEN IDV_METHOD IN ('FTF', 'PAV', 'DIDV', 'FTFP', 'NFTF', 'FTFT') OR IDV_METHOD IS NULL
                        THEN DATE(ID_EXPIRY_DATE)
                    WHEN IDV_METHOD = 'Credit File - Single Source'
                        THEN NULL
                    WHEN IDV_METHOD = 'Credit File - Dual Source' OR IDV_METHOD = 'Dual Source'
                        THEN NULL
                    END
                    """

    id_type = """CASE
                    WHEN IDV_METHOD IN ('FTF', 'PAV', 'DIDV', 'FTFP', 'NFTF', 'FTFT') OR IDV_METHOD IS NULL
                        THEN
                            CASE
                                WHEN ID_TYPE_DESCRIPTION IN ('PT', 'NOT-MATCHED', 'CAN-CITIZENSHIP-CARD', 'SC',
                                                   'SERVICE-CARD' , 'CC')
                                     THEN 'OTHER'
                                WHEN ID_TYPE_DESCRIPTION IN ('NATIONAL-IDENTITY-CARD' , 'CAN-CERT-OF-INDIAN-STATUS' , 'IS')
                                     THEN 'NATIONAL_ID_CARD'
                                WHEN ID_TYPE_DESCRIPTION IN ('HC' , 'HEALTH-CARD')
                                     THEN 'PROVINCIAL_HEALTH_CARD'
                                WHEN ID_TYPE_DESCRIPTION IN ('CAN-PASSPORT' , 'PS' , 'PASSPORT' , 'FOREIGN-PASSPORT')
                                     THEN 'PASSPORT'
                                WHEN ID_TYPE_DESCRIPTION IN ('DRIVER-LICENSE', 'DL')
                                     THEN 'DRIVERS_LICENSE'
                                WHEN ID_TYPE_DESCRIPTION IN ('PR' , 'CAN-PERMANENT-RESIDENT-CARD')
                                     THEN 'PERMANENT_RES_CARD'
                                ELSE
                                    'UNKNOWN'
                                END
                    WHEN IDV_METHOD = 'Credit File - Single Source'
                        THEN 'CREDIT_BUREAU'
                    WHEN IDV_METHOD = 'Credit File - Dual Source' OR IDV_METHOD = 'Dual Source'
                        THEN 'DUAL_PROCESS'
                    END
                          """

    query = f"""
            SELECT
            '320'                        AS INSTITUTION_NUMBER,
            CUSTOMER_NUMBER              AS CUSTOMER_NUMBER,
            {id_type}                    AS ID_TYPE,
            {id_type_description}        AS ID_TYPE_DESCRIPTION,
            NULLIF(ID_STATUS, '')        AS ID_STATUS,
            {id_number}                  AS ID_NUMBER,
            {id_state}                   AS ID_STATE,
            NULL                         AS ID_PROVINCE,
            {id_country}                 AS ID_COUNTRY,
            {id_issue_date}              AS ID_ISSUE_DATE,
            {id_expiry_date}             AS ID_EXPIRY_DATE,
            NULL                         AS ID_DATE_OF_BIRTH,
            NULL                         AS ALIEN_CERTIFICATION,
            NULLIF(IDV_METHOD, '')       AS IDV_METHOD,
            NULLIF(IDV_DECISION, '')     AS IDV_DECISION
            FROM {IDENTITY_TABLE};
    """
    logging.info(f"Identity feed extraction query : {query}")
    return query
