import logging
from aml_processing.adm_idv.tasks.adm_to_aml_idv_ftf_task import AdmToAmlIdvFTFTask
from aml_processing.adm_idv.adm_idv_common import ADMIdvConst
from aml_processing.transaction.bq_util import run_bq_dml_with_log
from aml_processing.adm_idv.tasks.adm_to_aml_idv_task import AdmToAmlIdvTask


class AdmToAmlIdvNFTFTPavTask(AdmToAmlIdvTask):
    # TEMP TABLES
    TBL_STG_APP_PAV_ID = f"pcb-{ADMIdvConst.DEPLOY_ENV}-processing.domain_aml.APP_PAV_ID"

    def __init__(self, dag_run_id: str):
        super().__init__(dag_run_id)

    def execute(self):
        logging.info('Started ADM NFTF Pav task')
        # Extracting APP_FTF_ID
        run_bq_dml_with_log('Creating APP_FTF_ID', 'Completed creating APP_FTF_ID', self.create_app_pav_id())
        logging.info('Completed  ADM NFTF PAV task')

    def create_app_pav_id(self) -> str:
        sql = f"""
                    CREATE OR REPLACE TABLE `{AdmToAmlIdvNFTFTPavTask.TBL_STG_APP_PAV_ID}` AS
                    SELECT
                        APP_NFTF.APA_APP_NUM                                    AS APP_NUM,
                        LTTR_XTO.TS2_ACCOUNT_ID                                 AS ACCOUNT_ID,
                        DISP_USER.ID_TYPE,
                        CUST_INFO.ID_NUMBER,
                        COALESCE(CUST_INFO.ISSUE_STATE,DISP_USER.JURISDICTION)  AS ISSUE_STATE,
                        DISP_USER.NATIONALITY,
                        DISP_USER.ISSUE_DATE,
                        DISP_USER.EXPIRY_DATE,
                        APP_NFTF.EXECUTION_ID,
                        'PAV'                                                   AS ID_METHOD,
                        'PAV'                                                   AS ID_DECISION
                    FROM
                        `{ADMIdvConst.TBL_STG_ADM_APP_NFTF}` APP_NFTF
                        INNER JOIN
                        (
                            SELECT
                                ADM_STD_DISP_USER_V.FILE_CREATE_DT,
                                ADM_STD_DISP_USER_V.APA_APP_NUM,
                                ADM_STD_DISP_USER_V.DTC_SEQUENCE,
                                ADM_STD_DISP_USER_V.REC_CREATE_TMS as UPDATE_DT,
                                ADM_STD_DISP_USER_V.EXECUTION_ID,
                                ADM_STD_DISP_USER_V.DTC_USER_2_BYTE_2 as ID_TYPE,
                                ADM_STD_DISP_USER_V.DTC_USER_5_BYTE_3 as JURISDICTION,
                                ADM_STD_DISP_USER_V.DTC_USER_5_BYTE_6 as NATIONALITY,
                                ADM_STD_DISP_USER_V.DTC_USER_10_BYTE_11 as EXPIRY_DATE,
                                ADM_STD_DISP_USER_V.DTC_USER_10_BYTE_12 as ISSUE_DATE,
                                ROW_NUMBER() OVER(
                                                    PARTITION BY
                                                        ADM_STD_DISP_USER_V.FILE_CREATE_DT,
                                                        ADM_STD_DISP_USER_V.APA_APP_NUM,
                                                        ADM_STD_DISP_USER_V.EXECUTION_ID
                                                    ORDER BY
                                                        ADM_STD_DISP_USER_V.REC_CREATE_TMS
                                                )AS REC_RANK
                            FROM
                                `{AdmToAmlIdvFTFTask.TBL_LANDING_ADM_STD_DISP_USER}` ADM_STD_DISP_USER_V
                            WHERE
                                ADM_STD_DISP_USER_V.DTC_SEQUENCE = 1
                        ) AS DISP_USER
                            ON DISP_USER.APA_APP_NUM = APP_NFTF.APA_APP_NUM
                            AND DISP_USER.EXECUTION_ID = APP_NFTF.EXECUTION_ID
                            AND DISP_USER.FILE_CREATE_DT = APP_NFTF.FILE_CREATE_DT
                            AND DISP_USER.REC_RANK = 1
                        INNER JOIN
                        (
                            SELECT
                                ADM_STD_DISP_CUST_INFO_V.FILE_CREATE_DT,
                                ADM_STD_DISP_CUST_INFO_V.APA_APP_NUM,
                                ADM_STD_DISP_CUST_INFO_V.DTC_SEQUENCE,
                                ADM_STD_DISP_CUST_INFO_V.REC_CREATE_TMS,
                                ADM_STD_DISP_CUST_INFO_V.DTC_DRIVERS_LICENSE_NUM as ID_NUMBER,
                                ADM_STD_DISP_CUST_INFO_V.DTC_DRIVERS_LICENSE_STATE as ISSUE_STATE,
                                ADM_STD_DISP_CUST_INFO_V.EXECUTION_ID,
                                ROW_NUMBER() OVER(
                                                    PARTITION BY
                                                        ADM_STD_DISP_CUST_INFO_V.FILE_CREATE_DT,
                                                        ADM_STD_DISP_CUST_INFO_V.APA_APP_NUM,
                                                        ADM_STD_DISP_CUST_INFO_V.EXECUTION_ID
                                                    ORDER BY
                                                        ADM_STD_DISP_CUST_INFO_V.REC_CREATE_TMS
                                                )AS REC_RANK
                            FROM
                                `{AdmToAmlIdvFTFTask.TBL_LANDING_ADM_ADM_STD_DISP_CUST_INFO}` ADM_STD_DISP_CUST_INFO_V
                            WHERE
                                ADM_STD_DISP_CUST_INFO_V.DTC_SEQUENCE = 1
                        ) AS CUST_INFO
                            ON DISP_USER.APA_APP_NUM = CUST_INFO.APA_APP_NUM
                            AND DISP_USER.EXECUTION_ID = CUST_INFO.EXECUTION_ID
                            AND DISP_USER.FILE_CREATE_DT = CUST_INFO.FILE_CREATE_DT
                            AND CUST_INFO.REC_RANK = 1
                        INNER JOIN
                        (
                            SELECT
                                ADM_STD_DISP_DT_ELMT_V.FILE_CREATE_DT,
                                ADM_STD_DISP_DT_ELMT_V.APA_APP_NUM,
                                ADM_STD_DISP_DT_ELMT_V.ELE_ELEMENT_NAME,
                                ADM_STD_DISP_DT_ELMT_V.ELE_ALPHA_VALUE,
                                ADM_STD_DISP_DT_ELMT_V.REC_CREATE_TMS,
                                ADM_STD_DISP_DT_ELMT_V.EXECUTION_ID,
                                ROW_NUMBER() OVER(
                                                    PARTITION BY
                                                        ADM_STD_DISP_DT_ELMT_V.FILE_CREATE_DT,
                                                        ADM_STD_DISP_DT_ELMT_V.EXECUTION_ID,
                                                        ADM_STD_DISP_DT_ELMT_V.APA_APP_NUM
                                                    ORDER BY
                                                        ADM_STD_DISP_DT_ELMT_V.REC_CREATE_TMS DESC
                                                )AS REC_RANK
                            FROM
                                `{ADMIdvConst.TBL_LANDING_ADM_STD_DISP_DT_ELMT}` ADM_STD_DISP_DT_ELMT_V
                            WHERE
                                ADM_STD_DISP_DT_ELMT_V.ELE_ELEMENT_NAME = 'ID_CHECK'
                                AND ADM_STD_DISP_DT_ELMT_V.ELE_ALPHA_VALUE = 'VALID'
                        ) AS DT_ELMT
                            ON DISP_USER.APA_APP_NUM = DT_ELMT.APA_APP_NUM
                            AND DISP_USER.EXECUTION_ID = DT_ELMT.EXECUTION_ID
                            AND DT_ELMT.FILE_CREATE_DT = APP_NFTF.FILE_CREATE_DT
                            AND DT_ELMT.REC_RANK = 1
                        INNER JOIN
                        (
                            SELECT
                                ADM_STD_DISP_DT_ELMT_V2.FILE_CREATE_DT,
                                ADM_STD_DISP_DT_ELMT_V2.APA_APP_NUM,
                                ADM_STD_DISP_DT_ELMT_V2.ELE_ELEMENT_NAME,
                                ADM_STD_DISP_DT_ELMT_V2.ELE_ALPHA_VALUE,
                                ADM_STD_DISP_DT_ELMT_V2.REC_CREATE_TMS,
                                ADM_STD_DISP_DT_ELMT_V2.EXECUTION_ID,
                                ROW_NUMBER() OVER(
                                                    PARTITION BY
                                                        ADM_STD_DISP_DT_ELMT_V2.FILE_CREATE_DT,
                                                        ADM_STD_DISP_DT_ELMT_V2.EXECUTION_ID,
                                                        ADM_STD_DISP_DT_ELMT_V2.APA_APP_NUM
                                                    ORDER BY
                                                        ADM_STD_DISP_DT_ELMT_V2.REC_CREATE_TMS DESC
                                                )AS REC_RANK
                            FROM
                                `{ADMIdvConst.TBL_LANDING_ADM_STD_DISP_DT_ELMT}` ADM_STD_DISP_DT_ELMT_V2
                            WHERE
                                ADM_STD_DISP_DT_ELMT_V2.ELE_ELEMENT_NAME = 'FTF_PAVILLION'
                                AND ADM_STD_DISP_DT_ELMT_V2.ELE_ALPHA_VALUE = 'Y'
                        ) AS PAV_DT_ELMT
                            ON DISP_USER.APA_APP_NUM = PAV_DT_ELMT.APA_APP_NUM
                            AND DISP_USER.EXECUTION_ID = PAV_DT_ELMT.EXECUTION_ID
                            AND PAV_DT_ELMT.FILE_CREATE_DT = APP_NFTF.FILE_CREATE_DT
                            AND PAV_DT_ELMT.REC_RANK = 1
                        LEFT OUTER JOIN
                        (
                            SELECT
                                LTTR_OVRD_DETAIL_XTO.APP_NUM,
                                LTTR_OVRD_DETAIL_XTO.REC_LOAD_TMS,
                                LTTR_OVRD_DETAIL_XTO.TS2_ACCOUNT_ID,
                                ROW_NUMBER() OVER(
                                                    PARTITION BY
                                                        LTTR_OVRD_DETAIL_XTO.APP_NUM
                                                    ORDER BY
                                                        LTTR_OVRD_DETAIL_XTO.FILE_CREATE_DT DESC,
                                                        LTTR_OVRD_DETAIL_XTO.REC_CHNG_TMS DESC
                                                )AS REC_RANK
                            FROM
                                `{AdmToAmlIdvFTFTask.TBL_LANDING_ADM_ADM_LTTR_OVRD_DETAIL_XTO}` LTTR_OVRD_DETAIL_XTO
                            WHERE
                                LTTR_OVRD_DETAIL_XTO.TS2_ACCOUNT_ID IS NOT NULL
                                AND LTTR_OVRD_DETAIL_XTO.TS2_ACCOUNT_ID <> 0
                        ) AS LTTR_XTO
                            ON LTTR_XTO.APP_NUM = APP_NFTF.APA_APP_NUM
                            AND LTTR_XTO.REC_RANK = 1
                    WHERE CUST_INFO.ID_NUMBER IS NOT NULL AND CUST_INFO.ID_NUMBER != ''
                """
        return sql
