from aml_processing.adm_idv.adm_idv_common import ADMIdvConst
from aml_processing.adm_idv.tasks.adm_to_aml_idv_task import AdmToAmlIdvTask
from aml_processing.transaction.bq_util import run_bq_dml_with_log
from google.cloud import bigquery


class AdmToAmlIdvCreateInitStgTablesTask(AdmToAmlIdvTask):

    def __init__(self, dag_run_id: str):
        super().__init__(dag_run_id)

    def execute(self):
        self.create_stg_ftf()
        self.create_stg_nftf()
        self.create_stg_gen_val_detail_nftf()
        self.create_stg_dt_elmt()
        self.create_stg_lttr_ovrd_detail_xto()

    def create_stg_ftf(self):
        sql = f"""CREATE OR REPLACE TABLE `{ADMIdvConst.TBL_STG_ADM_APP_FTF}` AS
                SELECT DISTINCT
                   e.APA_APP_NUM,
                   e.ELE_ELEMENT_NAME,
                   e.ELE_ALPHA_VALUE,
                   e.EXECUTION_ID,
                   e.FILE_CREATE_DT
                FROM
                   {ADMIdvConst.TBL_LANDING_ADM_STD_DISP_DT_ELMT} e
                    INNER JOIN `{ADMIdvConst.TBL_ADM_CTL}` c ON c.FILE_CREATE_DT = e.FILE_CREATE_DT
                                                                            AND c.EXECUTION_ID = e.EXECUTION_ID
                                                                            AND c.APA_APP_NUM = e.APA_APP_NUM
                                                                            AND c.RUN_ID = @dag_run_id
                                                                            AND c.LATEST_VERSION = 'Y'
                WHERE e.ELE_ELEMENT_NAME IN ('FTF_NFTF_INDICATOR')
                     AND e.ELE_ALPHA_VALUE = 'FTF'
                     AND e.FILE_CREATE_DT in UNNEST (@file_create_dt_list)
        """

        file_create_dt_list = self.get_file_create_dates()
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("dag_run_id", "STRING", self.dag_run_id),
                bigquery.ArrayQueryParameter("file_create_dt_list", "DATE", file_create_dt_list)

            ]
        )
        run_bq_dml_with_log('Creating adm staging FTF table', 'Completed creating adm staging FTF table', sql, job_config)

    def create_stg_nftf(self):
        sql = f"""CREATE OR REPLACE TABLE `{ADMIdvConst.TBL_STG_ADM_APP_NFTF}` AS
                SELECT DISTINCT
                   e.APA_APP_NUM,
                   e.ELE_ELEMENT_NAME,
                   e.ELE_ALPHA_VALUE,
                   e.EXECUTION_ID,
                   e.FILE_CREATE_DT
                FROM
                   {ADMIdvConst.TBL_LANDING_ADM_STD_DISP_DT_ELMT} e
                    INNER JOIN `{ADMIdvConst.TBL_ADM_CTL}` c ON c.FILE_CREATE_DT = e.FILE_CREATE_DT
                                                                             AND c.EXECUTION_ID = e.EXECUTION_ID
                                                                            AND c.APA_APP_NUM = e.APA_APP_NUM
                                                                            AND c.RUN_ID = '{self.dag_run_id}'
                                                                            AND c.LATEST_VERSION = 'Y'
                WHERE e.ELE_ELEMENT_NAME IN ('FTF_NFTF_INDICATOR')
                     AND e.ELE_ALPHA_VALUE = 'NFTF'
                    AND e.FILE_CREATE_DT in UNNEST (@file_create_dt_list)
        """

        file_create_dt_list = self.get_file_create_dates()
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("dag_run_id", "STRING", self.dag_run_id),
                bigquery.ArrayQueryParameter("file_create_dt_list", "DATE", file_create_dt_list)

            ]
        )
        run_bq_dml_with_log('Creating adm staging NFTF table', 'Completed creating adm staging NFTF table', sql, job_config)

    def create_stg_dt_elmt(self):
        sql = f"""CREATE OR REPLACE TABLE `{ADMIdvConst.TBL_STG_ADM_DT_ELMT}` AS
                 SELECT x.APA_APP_NUM,
                        x.ELE_ELEMENT_NAME,
                        x.ELE_ALPHA_VALUE,
                        x.EXECUTION_ID,
                        x.REC_CREATE_TMS,
                        x.REC_CHANGE_TMS,
                        x.FILE_CREATE_DT
                     FROM (SELECT
                               e.APA_APP_NUM,
                               e.ELE_ELEMENT_NAME,
                               e.ELE_ALPHA_VALUE,
                               e.EXECUTION_ID,
                               e.REC_CREATE_TMS,
                               e.REC_CHANGE_TMS,
                               e.FILE_CREATE_DT,
                               row_number() OVER(PARTITION BY e.APA_APP_NUM,e.ELE_ELEMENT_NAME
                                                ORDER BY e.FILE_CREATE_DT DESC, e.EXECUTION_ID DESC, e.REC_CHANGE_TMS DESC) REC_RANK
                            FROM
                               {ADMIdvConst.TBL_LANDING_ADM_STD_DISP_DT_ELMT} e
                                INNER JOIN `{ADMIdvConst.TBL_ADM_CTL}` c ON c.FILE_CREATE_DT = e.FILE_CREATE_DT
                                                                                        AND c.EXECUTION_ID = e.EXECUTION_ID
                                                                                        AND c.APA_APP_NUM = e.APA_APP_NUM
                                                                                        AND c.RUN_ID = @dag_run_id
                                                                                        AND c.LATEST_VERSION = 'Y'
                            WHERE e.FILE_CREATE_DT in UNNEST (@file_create_dt_list)
                       ) x
                WHERE x.REC_RANK = 1
        """

        file_create_dt_list = self.get_file_create_dates()
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("dag_run_id", "STRING", self.dag_run_id),
                bigquery.ArrayQueryParameter("file_create_dt_list", "DATE", file_create_dt_list)

            ]
        )
        run_bq_dml_with_log(f'Creating adm staging {ADMIdvConst.TBL_STG_ADM_DT_ELMT}', f'Completed creating adm staging {ADMIdvConst.TBL_STG_ADM_DT_ELMT}', sql, job_config)

    def create_stg_gen_val_detail_nftf(self):
        sql = f"""
        CREATE OR REPLACE TABLE `{ADMIdvConst.TBL_STG_GEN_VAL_DETAIL_NFTF}` as
        SELECT d.FILE_CREATE_DT,
               d.EXECUTION_ID,
               d.APP_NUM,
               d.ELEMENT_NAME,
               d.ALPHA_VALUE,
               d.GVL_DATE,
               d.COMPONENT_NAME,
               d.GVL_PRODUCT,
               d.NUMERIC_VALUE,
               d.REC_CHNG_TMS
        FROM `{ADMIdvConst.TBL_LANDING_GEN_VAL_DETAIL}` as d
             INNER JOIN `{ADMIdvConst.TBL_STG_ADM_APP_NFTF}` m
             on d.APP_NUM = m.APA_APP_NUM
        """
        run_bq_dml_with_log(f"Creating staging table:{ADMIdvConst.TBL_STG_GEN_VAL_DETAIL_NFTF}",
                            f"Completed creating staging table:{ADMIdvConst.TBL_STG_GEN_VAL_DETAIL_NFTF}",
                            sql)

    def create_stg_lttr_ovrd_detail_xto(self):
        sql = f"""CREATE OR REPLACE TABLE `{ADMIdvConst.TBL_STG_LTTR_OVRD_DETAIL_XTO}` AS
                SELECT LTTR_OVRD_DETAIL_XTO.APP_NUM,
                       LTTR_OVRD_DETAIL_XTO.TS2_ACCOUNT_ID,
                       row_number() OVER(PARTITION BY APP_NUM ORDER BY LTTR_OVRD_DETAIL_XTO.FILE_CREATE_DT DESC, LTTR_OVRD_DETAIL_XTO.REC_CHNG_TMS DESC) REC_RANK
                FROM `{ADMIdvConst.TBL_LANDING_LTTR_OVRD_DETAIL_XTO}` LTTR_OVRD_DETAIL_XTO
                    INNER JOIN `{ADMIdvConst.TBL_ADM_CTL}` c ON c.APA_APP_NUM = LTTR_OVRD_DETAIL_XTO.APP_NUM
                                                                            AND c.RUN_ID = @dag_run_id
                                                                            AND c.LATEST_VERSION = 'Y'
                WHERE  LTTR_OVRD_DETAIL_XTO.TS2_ACCOUNT_ID IS NOT NULL
                     AND LTTR_OVRD_DETAIL_XTO.TS2_ACCOUNT_ID <> 0
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("dag_run_id", "STRING", self.dag_run_id)
            ]
        )
        run_bq_dml_with_log(f'Creating adm staging {ADMIdvConst.TBL_STG_LTTR_OVRD_DETAIL_XTO}',
                            f'Completed creating adm staging {ADMIdvConst.TBL_STG_LTTR_OVRD_DETAIL_XTO}', sql, job_config)
