from aml_processing.adm_idv.tasks.adm_to_aml_idv_task import AdmToAmlIdvTask
from aml_processing.adm_idv.adm_idv_common import ADMIdvConst
from aml_processing.transaction.bq_util import run_bq_dml_with_log
from datetime import date
from google.cloud import bigquery


class AdmToAmlIdvCreateCtlTask(AdmToAmlIdvTask):

    def __init__(self, dag_run_id: str, cutoff_date: date):
        super().__init__(dag_run_id)
        self.cutoff_date = cutoff_date

    def execute(self):
        run_bq_dml_with_log('Creating adm_idv_info', 'Completed creating adm_idv_info', self.get_create_curated_aml_idv_info_sql())
        run_bq_dml_with_log('Creating adm control table', 'Completed creating adm control table', self.get_create_sql())
        self.insert_into_ctl_table()
        run_bq_dml_with_log('Updating adm control table latest records', 'Completed updating adm control table', self.get_update_sql())

        run_bq_dml_with_log('Creating adm digital source control table', 'Completed creating adm digital source control table', self.get_create_adm_digital_ctl_sql())
        run_bq_dml_with_log('Creating adm digital source control initial record if needed', 'Completed creating adm digital source control initial record', self.get_init_adm_digital_ctl_sql())
        run_bq_dml_with_log('Creating adm exception table', 'Completed creating adm exception table', self.get_creat_adm_idv_excp_sql())

    def get_create_sql(self) -> str:
        sql = f""" CREATE TABLE IF NOT EXISTS `{ADMIdvConst.TBL_ADM_CTL}` (
              RUN_ID STRING,
              FILE_CREATE_DT DATE,
              EXECUTION_ID NUMERIC,
              APA_APP_NUM NUMERIC,
              LATEST_VERSION STRING,
              STATUS STRING
              )
            """
        return sql

    def insert_into_ctl_table(self):
        sql = f"""INSERT INTO `{ADMIdvConst.TBL_ADM_CTL}`(RUN_ID,FILE_CREATE_DT,EXECUTION_ID,APA_APP_NUM,LATEST_VERSION,STATUS)
                SELECT '{self.dag_run_id}',a.FILE_CREATE_DT,a.EXECUTION_ID,a.APA_APP_NUM,'N','{ADMIdvConst.CTL_STATUS_IN_PROGRESS}'
                FROM `{ADMIdvConst.TBL_LANDING_ADM_STD_DISP_APP_DATA}`  a
                WHERE  a.APA_STATUS = 'NEWACCOUNT'
                   AND a.APA_QUEUE_ID IN ('APPROVE','A2 WAIT')
                   AND (a.APA_TEST_ACCOUNT_FLAG IS NULL OR a.APA_TEST_ACCOUNT_FLAG = '')
                   AND NOT EXISTS ( SELECT 1
                                   FROM `{ADMIdvConst.TBL_CURATED_AML_IDV_INFO}` c
                                   WHERE c.APP_NUM = a.APA_APP_NUM
                                   )
                   AND a.FILE_CREATE_DT > @cutoff_date
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("cutoff_date", "DATE", self.cutoff_date.strftime("%Y-%m-%d"))])
        run_bq_dml_with_log(f'Inserting into {ADMIdvConst.TBL_ADM_CTL}',
                            f'Completed inserting into {ADMIdvConst.TBL_ADM_CTL}',
                            sql, job_config)

    def get_update_sql(self) -> str:
        sql = f"""UPDATE `{ADMIdvConst.TBL_ADM_CTL}` C
                     SET C.LATEST_VERSION = CASE WHEN CR.R_LATEST = 1 THEN 'Y' ELSE 'N' END
                     FROM (
                           SELECT RUN_ID
                                ,EXECUTION_ID
                                ,APA_APP_NUM
                                ,FILE_CREATE_DT
                               ,row_number() OVER(PARTITION BY APA_APP_NUM ORDER BY EXECUTION_ID DESC) R_LATEST
                           FROM `{ADMIdvConst.TBL_ADM_CTL}`
                           WHERE RUN_ID = '{self.dag_run_id}'
                         ) CR
                     WHERE CR.EXECUTION_ID = C.EXECUTION_ID
                          AND CR.FILE_CREATE_DT = C.FILE_CREATE_DT
                          AND CR.APA_APP_NUM= C.APA_APP_NUM
                          AND C.RUN_ID = CR.RUN_ID
                          AND C.RUN_ID = '{self.dag_run_id}'
                """
        return sql

    def get_create_adm_digital_ctl_sql(self) -> str:
        sql = f""" CREATE TABLE IF NOT EXISTS `{ADMIdvConst.TBL_ADM_DIGITAL_CTL}` (
              RUN_ID STRING,
              LAST_PROCESS_TIME DATETIME
              )
            """
        return sql

    def get_init_adm_digital_ctl_sql(self) -> str:
        # Create an initial record for the first run, each run would update the record with its run_id and run timestamp.

        sql = f""" INSERT INTO `{ADMIdvConst.TBL_ADM_DIGITAL_CTL}` (
                  RUN_ID, LAST_PROCESS_TIME)
                  SELECT '{self.dag_run_id}',parse_datetime('%Y-%m-%d', '1971-01-01')
                  FROM (SELECT 1)
                  WHERE NOT EXISTS ( SELECT 1 FROM `{ADMIdvConst.TBL_ADM_DIGITAL_CTL}`)
                """
        return sql

    def get_creat_adm_idv_excp_sql(self) -> str:
        sql = f"""
              CREATE TABLE  IF NOT EXISTS `{ADMIdvConst.TBL_AML_IDV_EXCP}` (
              RUN_ID STRING NOT NULL,
              APP_NUM NUMERIC NOT NULL,
              EXCEPTION_RSN STRING NOT NULL,
              REC_CREATE_DT DATETIME NOT NULL
              )
             """
        return sql

    def get_create_curated_aml_idv_info_sql(self):
        sql = f"""
            CREATE TABLE IF NOT EXISTS `{ADMIdvConst.TBL_CURATED_AML_IDV_INFO}`(
            CUSTOMER_NUMBER	        STRING,
            APP_NUM	                NUMERIC,
            ID_TYPE	                STRING,
            ID_TYPE_DESCRIPTION	    STRING,
            ID_STATUS	            STRING,
            ID_NUMBER	            STRING,
            ID_STATE	            STRING,
            ID_COUNTRY	            STRING,
            ID_ISSUE_DATE	        STRING,
            ID_EXPIRY_DATE	        STRING,
            NAME_ON_SOURCE	        STRING,
            REF_NUMBER	            STRING,
            DATE_VERIFIED	        STRING,
            TYPE_OF_INFO	        STRING,
            IDV_METHOD	            STRING,
            IDV_DECISION	        STRING,
            REC_CREATE_TMS	        DATETIME NOT NULL,
            RUN_ID	                STRING NOT NULL
            )
            """
        return sql
