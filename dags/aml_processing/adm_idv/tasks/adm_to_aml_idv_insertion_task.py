from aml_processing.adm_idv.adm_idv_common import ADMIdvConst
from aml_processing.adm_idv.tasks.adm_to_aml_idv_task import AdmToAmlIdvTask
from aml_processing.adm_idv.tasks.adm_to_aml_idv_ftf_task import AdmToAmlIdvFTFTask
from aml_processing.adm_idv.tasks.adm_to_aml_idv_nftf_pav_task import AdmToAmlIdvNFTFTPavTask
from aml_processing.transaction.bq_util import run_bq_dml_with_log
import logging

# This class implements the execute() method which has scripts corresponding to UNION of data from all 4 methods(FTF , PAV , NFTF Single Source & NFTF Dual Source)


class AdmToAmlIdvInsertionTask(AdmToAmlIdvTask):
    TBL_STG_ACC_CUST = f"pcb-{ADMIdvConst.DEPLOY_ENV}-processing.domain_aml.ACC_CUST"
    TBL_STG_ADM_NFTF_DS_EXP = f"pcb-{ADMIdvConst.DEPLOY_ENV}-processing.domain_aml.STG_ADM_NFTF_DS_EXCP"

    # Generic SQLs for insertions into AML_IDV_INFO and AML_IDV_EXCP
    # AML_IDV_INFO
    insert_aml_idv_info = f"""
                            INSERT INTO {ADMIdvConst.TBL_AGG_AML_IDV_INFO}
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
                                NAME_ON_SOURCE,
                                REF_NUMBER,
                                DATE_VERIFIED,
                                TYPE_OF_INFO,
                                IDV_METHOD,
                                IDV_DECISION,
                                REC_CREATE_TMS,
                                RUN_ID
                            )
                            """

    idv_unique_condition = f"""
                                AND NOT EXISTS(
                                        SELECT
                                            IDV.CUSTOMER_NUMBER,
                                            IDV.APP_NUM
                                        FROM
                                            {ADMIdvConst.TBL_AGG_AML_IDV_INFO} IDV
                                        WHERE
                                            IDV.CUSTOMER_NUMBER = ACC_CUST.CUSTOMER_IDENTIFIER_NO
                                            AND IDV.APP_NUM = stg_tbl.APP_NUM
                                    )
                            """

    ftf_pav_filter = """
                                    (
                                        stg_tbl.ID_NUMBER IS NOT NULL
                                        AND stg_tbl.ACCOUNT_ID IS NOT NULL
                                    )
                                """

    nftf_ss_ds_filter = """
                                    (
                                        stg_tbl.REF_NUMBER IS NOT NULL
                                        AND stg_tbl.ACCOUNT_ID IS NOT NULL
                                    )
                                """

    # AML_IDV_EXCP
    insert_exception_idv = f"""
                                INSERT INTO `{ADMIdvConst.TBL_AML_IDV_EXCP}`
                                (
                                APP_NUM,
                                EXCEPTION_RSN,
                                REC_CREATE_DT,
                                RUN_ID
                                )
                            """

    exception_unique_condition = f"""
                                    AND NOT EXISTS (
                                            SELECT
                                                excp.APP_NUM
                                            FROM
                                                `{ADMIdvConst.TBL_AML_IDV_EXCP}` excp
                                            WHERE
                                                excp.APP_NUM = stg_tbl.APP_NUM
                                        )
                                """

    ftf_pav_exception_filter = """
                                    (
                                        stg_tbl.ID_NUMBER IS NULL
                                        OR stg_tbl.ACCOUNT_ID IS NULL
                                    )
                                """

    nftf_ss_ds_exception_filter = """
                                    (
                                        stg_tbl.REF_NUMBER IS NULL
                                        OR stg_tbl.ACCOUNT_ID IS NULL
                                    )
                                """

    case_statement_exception_ftf_pav = """
                                            CASE
                                                WHEN
                                                    stg_tbl.ID_NUMBER IS NULL
                                                THEN
                                                    'ID Number is missing'
                                                WHEN
                                                    stg_tbl.ACCOUNT_ID IS NULL
                                                THEN
                                                    'Account Id is missing'
                                                ELSE
                                                    'Data Missing'
                                            END
                                        """

    case_statement_exception_nftf_ss_ds = """
                                                CASE
                                                    WHEN
                                                        stg_tbl.REF_NUMBER IS NULL
                                                    THEN
                                                        'Reference Number is missing'
                                                    WHEN
                                                        stg_tbl.ACCOUNT_ID IS NULL
                                                    THEN
                                                        'Account Id is missing'
                                                    ELSE
                                                        'Data Missing'
                                                END
                                            """

    def __init__(self, dag_run_id: str):
        self.dag_run_id = dag_run_id

    def execute(self):
        logging.info('Starting the ADM to IDV Insertion Task')
        # Creating the base dataset for mapping ACCOUNT_ID to CUSTOMER_NUMBER
        run_bq_dml_with_log(f"Creating base dataset staging table:{AdmToAmlIdvInsertionTask.TBL_STG_ACC_CUST}",
                            f"Completed creating base dataset staging table:{AdmToAmlIdvInsertionTask.TBL_STG_ACC_CUST}", self.create_acc_cust())

        # INSERTING INTO AML_IDV_INFO FROM ALL 4 METHODS
        # Inserting into AML_IDV_INFO from APP_FTF_ID
        run_bq_dml_with_log(f"Inserting into AML_IDV_INFO from :{ADMIdvConst.TBL_STG_APP_FTF_ID}",
                            f"Completed inserting from staging table:{ADMIdvConst.TBL_STG_APP_FTF_ID}", self.insert_into_aml_idv_info(ADMIdvConst.TBL_STG_APP_FTF_ID))
        # Inserting into AML_IDV_INFO from APP_PAV_ID
        run_bq_dml_with_log(f"Inserting into AML_IDV_INFO from :{ADMIdvConst.TBL_STG_APP_PAV_ID}",
                            f"Completed inserting from staging table:{ADMIdvConst.TBL_STG_APP_PAV_ID}", self.insert_into_aml_idv_info(ADMIdvConst.TBL_STG_APP_PAV_ID))
        # Inserting into AML_IDV_INFO from NFTF_SS_ID
        run_bq_dml_with_log(f"Inserting into AML_IDV_INFO from :{ADMIdvConst.TBL_STG_ADM_NFTF_SS_FINAL}",
                            f"Completed inserting from staging table:{ADMIdvConst.TBL_STG_ADM_NFTF_SS_FINAL}", self.insert_into_aml_idv_info(ADMIdvConst.TBL_STG_ADM_NFTF_SS_FINAL))
        # Inserting into AML_IDV_INFO from NFTF_DS_ID
        run_bq_dml_with_log(f"Inserting into AML_IDV_INFO from :{ADMIdvConst.TBL_STG_ADM_NFTF_DS_FINAL}",
                            f"Completed inserting from staging table:{ADMIdvConst.TBL_STG_ADM_NFTF_DS_FINAL}", self.insert_into_aml_idv_info(ADMIdvConst.TBL_STG_ADM_NFTF_DS_FINAL))
        logging.info('Finished the ADM to IDV Union Task')

        # INSERTING INTO AML_IDV_EXCP FROM ALL 4 METHODS
        # Inserting into AML_IDV_EXCP from APP_FTF_ID
        run_bq_dml_with_log(f"Inserting into AML_IDV_EXCP from :{ADMIdvConst.TBL_STG_APP_FTF_ID}",
                            f"Completed inserting from staging table:{ADMIdvConst.TBL_STG_APP_FTF_ID}", self.insert_into_aml_idv_excp_from_tbl_stg_app_ftf_id(ADMIdvConst.TBL_STG_APP_FTF_ID))
        # Inserting into AML_IDV_EXCP from APP_PAV_ID
        run_bq_dml_with_log(f"Inserting into AML_IDV_EXCP from :{ADMIdvConst.TBL_STG_APP_PAV_ID}",
                            f"Completed inserting from staging table:{ADMIdvConst.TBL_STG_APP_PAV_ID}", self.insert_into_aml_idv_excp_from_tbl_stg_app_ftf_id(ADMIdvConst.TBL_STG_APP_PAV_ID))
        # Inserting into AML_IDV_EXCP from NFTF_SS_ID
        run_bq_dml_with_log(f"Inserting into AML_IDV_EXCP from :{ADMIdvConst.TBL_STG_ADM_NFTF_SS_FINAL}",
                            f"Completed inserting from staging table:{ADMIdvConst.TBL_STG_ADM_NFTF_SS_FINAL}", self.insert_into_aml_idv_excp_from_tbl_stg_app_ftf_id(ADMIdvConst.TBL_STG_ADM_NFTF_SS_FINAL))
        # Inserting into AML_IDV_EXCP from NFTF_SS_ID
        run_bq_dml_with_log(f"Inserting into AML_IDV_EXCP from :{ADMIdvConst.TBL_STG_ADM_NFTF_DS_FINAL}",
                            f"Completed inserting from staging table:{ADMIdvConst.TBL_STG_ADM_NFTF_DS_FINAL}", self.insert_into_aml_idv_excp_from_tbl_stg_app_ftf_id(ADMIdvConst.TBL_STG_ADM_NFTF_DS_FINAL))

    # Method to create the the base dataset for mapping ACCOUNT_ID to CUSTOMER_NUMBER
    def create_acc_cust(self):
        sql = f"""  CREATE OR REPLACE TABLE `{AdmToAmlIdvInsertionTask.TBL_STG_ACC_CUST}` AS
                    SELECT
                        SAFE_CAST(ACCOUNT.ACCOUNT_NO AS INT) AS ACCOUNT_NO,
                        CUSTOMER_IDENTIFIER.CUSTOMER_IDENTIFIER_NO
                    FROM
                        `pcb-{ADMIdvConst.DEPLOY_ENV}-landing.domain_account_management.ACCOUNT` ACCOUNT
                        INNER JOIN `pcb-{ADMIdvConst.DEPLOY_ENV}-landing.domain_account_management.ACCOUNT_CUSTOMER` ACCOUNT_CUSTOMER
                            ON ACCOUNT.ACCOUNT_UID=ACCOUNT_CUSTOMER.ACCOUNT_UID
                        INNER JOIN `pcb-{ADMIdvConst.DEPLOY_ENV}-landing.domain_customer_management.CUSTOMER_IDENTIFIER` CUSTOMER_IDENTIFIER
                            ON ACCOUNT_CUSTOMER.CUSTOMER_UID = CUSTOMER_IDENTIFIER.CUSTOMER_UID
                    WHERE
                        ACCOUNT_CUSTOMER.ACCOUNT_CUSTOMER_ROLE_UID = 1
                        AND CUSTOMER_IDENTIFIER.TYPE = 'PCF-CUSTOMER-ID'
                        AND CUSTOMER_IDENTIFIER.DISABLED_IND = 'N'
                        AND ACCOUNT_CUSTOMER.ACTIVE_IND = 'Y'
                """
        return sql

    def insert_into_aml_idv_info(self, staging_table):
        # For NFTF Single Source and Dual Source
        id_type_description = 'stg_tbl.ID_TYPE_DESCRIPTION'
        id_number = 'NULL'
        id_state = 'NULL'
        id_country = 'stg_tbl.ID_COUNTRY'
        issue_date = 'NULL'
        expiry_date = 'NULL'
        name_on_source = 'stg_tbl.NAME_ON_SOURCE'
        ref_number = 'stg_tbl.REF_NUMBER'
        date_verified = 'SAFE_CAST(stg_tbl.DATE_VERIFIED AS STRING)'
        type_of_info = 'stg_tbl.NAME_ON_SOURCE'
        idv_decision = 'stg_tbl.IDV_DECISION'
        idv_method = 'stg_tbl.IDV_METHOD'
        stg_tbl_join_condition = AdmToAmlIdvInsertionTask.nftf_ss_ds_filter

        if (staging_table in [AdmToAmlIdvFTFTask.TBL_STG_APP_FTF_ID, AdmToAmlIdvNFTFTPavTask.TBL_STG_APP_PAV_ID]):
            # For FTF and PAV
            id_type_description = 'stg_tbl.ID_TYPE'
            id_number = 'stg_tbl.ID_NUMBER'
            id_state = 'stg_tbl.ISSUE_STATE'
            id_country = 'stg_tbl.NATIONALITY'
            issue_date = 'SAFE_CAST(stg_tbl.ISSUE_DATE AS STRING)'
            expiry_date = 'SAFE_CAST(stg_tbl.EXPIRY_DATE AS STRING)'
            name_on_source = 'NULL'
            ref_number = 'NULL'
            date_verified = 'NULL'
            type_of_info = 'NULL'
            idv_method = 'stg_tbl.ID_METHOD'
            idv_decision = 'stg_tbl.ID_DECISION'
            stg_tbl_join_condition = AdmToAmlIdvInsertionTask.ftf_pav_filter

        sql = f"""
                    {AdmToAmlIdvInsertionTask.insert_aml_idv_info}
                    SELECT
                        ACC_CUST.CUSTOMER_IDENTIFIER_NO         AS CUSTOMER_NUMBER,
                        stg_tbl.APP_NUM,
                        stg_tbl.ID_TYPE,
                        {id_type_description}                   AS ID_TYPE_DESCRIPTION,
                        'PRIMARY'                               AS ID_STATUS,
                        {id_number}                             AS ID_NUMBER,
                        {id_state}                              AS ID_STATE,
                        {id_country}                            AS ID_COUNTRY,
                        {issue_date}                            AS ID_ISSUE_DATE,
                        {expiry_date}                           AS ID_EXPIRY_DATE,
                        {name_on_source}                        AS NAME_ON_SOURCE,
                        {ref_number}                            AS REF_NUMBER,
                        {date_verified}                         AS DATE_VERIFIED,
                        {type_of_info}                          AS TYPE_OF_INFO,
                        {idv_method}                            AS IDV_METHOD,
                        {idv_decision}                          AS IDV_DECISION,
                        CURRENT_DATETIME()                      AS REC_CREATE_TMS,
                        '{self.dag_run_id}'                     AS RUN_ID
                    FROM
                        {staging_table} stg_tbl
                        INNER JOIN `{AdmToAmlIdvInsertionTask.TBL_STG_ACC_CUST}` ACC_CUST
                            ON stg_tbl.ACCOUNT_ID = ACC_CUST.ACCOUNT_NO
                    WHERE
                        {stg_tbl_join_condition}
                        {AdmToAmlIdvInsertionTask.idv_unique_condition}
                """
        return sql

    def insert_into_aml_idv_excp_from_tbl_stg_app_ftf_id(self, staging_table):
        # For NFTF Single Source and Dual Source
        case_statement = AdmToAmlIdvInsertionTask.case_statement_exception_nftf_ss_ds
        excp_filter = AdmToAmlIdvInsertionTask.nftf_ss_ds_exception_filter

        if (staging_table in [ADMIdvConst.TBL_STG_APP_FTF_ID, ADMIdvConst.TBL_STG_APP_PAV_ID]):
            # For FTF and PAV
            case_statement = AdmToAmlIdvInsertionTask.case_statement_exception_ftf_pav
            excp_filter = AdmToAmlIdvInsertionTask.ftf_pav_exception_filter
        elif (staging_table == ADMIdvConst.TBL_STG_ADM_NFTF_DS_FINAL):
            staging_table = AdmToAmlIdvInsertionTask.TBL_STG_ADM_NFTF_DS_EXP
        sql = f"""
                    {AdmToAmlIdvInsertionTask.insert_exception_idv}
                    SELECT
                        stg_tbl.APP_NUM,
                        {case_statement}            AS EXCEPTION_RSN,
                        CURRENT_DATETIME()          AS REC_CREATE_DT,
                        '{self.dag_run_id}'         AS RUN_ID
                    FROM
                        {staging_table} stg_tbl
                    WHERE
                        {excp_filter}
                        {AdmToAmlIdvInsertionTask.exception_unique_condition}
                """
        return sql
