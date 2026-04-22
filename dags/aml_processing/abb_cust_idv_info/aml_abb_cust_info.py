import logging

from datetime import datetime

from aml_processing.transaction.bq_util import run_bq_dml_with_log
from util.miscutils import read_variable_or_file
from aml_processing import feed_commons as commons

DEPLOY_ENV = read_variable_or_file('gcp_config')['deployment_environment_name']
# These are for reference only. Any change has to be made in the corresponding .sql files
TBL_CURATED_ADM_STD_DISP_USER = f"pcb-{DEPLOY_ENV}-curated.domain_customer_acquisition.ADM_STD_DISP_USER"
TBL_CURATED_ADM_STD_DISP_APP_DATA = f"pcb-{DEPLOY_ENV}-curated.domain_customer_acquisition.ADM_STD_DISP_APP_DATA"
TBL_CURATED_ADM_STD_DISP_DT_ELMT = f"pcb-{DEPLOY_ENV}-curated.domain_customer_acquisition.ADM_STD_DISP_DT_ELMT"
TBL_CURATED_ADM_STD_DISP_BAL_TRNS_INFO = f"pcb-{DEPLOY_ENV}-curated.domain_customer_acquisition.ADM_STD_DISP_BAL_TRNS_INFO"
TBL_PROC_APP_ABB = f"pcb-{DEPLOY_ENV}-processing.domain_aml.APP_ABB"
TBL_PROC_APP_ABB_ACC = f"pcb-{DEPLOY_ENV}-processing.domain_aml.APP_ABB_ACC"
TBL_PROC_STG_ABB_CUST = f"pcb-{DEPLOY_ENV}-processing.domain_aml.STG_ABB_CUST"
TBL_CURATED_AML_ABB_CUST = f"pcb-{DEPLOY_ENV}-curated.domain_aml.AML_ABB_CUST"
TBL_CURATED_ACCOUNT = f"pcb-{DEPLOY_ENV}-curated.domain_account_management.ACCOUNT"
TBL_CURATED_ACCOUNT_CUSTOMER = f"pcb-{DEPLOY_ENV}-curated.domain_account_management.ACCOUNT_CUSTOMER"
TBL_CURATED_CUSTOMER_IDENTIFIER = f"pcb-{DEPLOY_ENV}-curated.domain_customer_management.CUSTOMER_IDENTIFIER"
TBL_CURATED_ADM_ADM_LTTR_OVRD_DETAIL_XTO = f"pcb-{DEPLOY_ENV}-curated.domain_customer_acquisition.LTTR_OVRD_DETAIL_XTO"
TBL_CURATED_CUST_ID_VERF = f"pcb-{DEPLOY_ENV}-curated.domain_validation_verification.CUSTOMER_ID_VERIFICATION"
AGG_CUST_IDV_INFO = f"pcb-{DEPLOY_ENV}-curated.domain_aml.CUST_IDV_INFO"


class ABBIdvInfo:

    def task_create_app_abb_table(self, sql_file_path, **context):
        sql = commons.read_sql_file(sql_file_path).replace('{DEPLOY_ENV}', DEPLOY_ENV)
        if context['params'] is not None and context['params'].get('start_date') is not None:
            start_date = context['params']['start_date']
            logging.info(f"The provided start date is {start_date}")
        else:
            start_date = datetime.now().date()
            logging.info(f"The start date is not provided, hence running default start date is : {start_date}")
        sql_query = sql.replace('{start_date}', str(start_date))
        run_bq_dml_with_log('Creating APP_ABB table', 'Completed creating processing APP_ABB table', sql_query)

    def task_create_app_abb_acc_table(self, sql_file_path):
        self.run_abb_bq_queries(sql_file_path, 'APP_ABB_ACC', "Creating")

    def task_create_stg_abb_cust_table(self, sql_file_path):
        self.run_abb_bq_queries(sql_file_path, 'STG_ABB_CUST', "Creating")

    def task_create_aml_abb_cust(self, sql_file_path_create, sql_file_path_insert):
        self.run_abb_bq_queries(sql_file_path_create, 'CURATED_AML_ABB_CUST', "Creating")
        self.run_abb_bq_queries(sql_file_path_insert, 'CURATED_AML_ABB_CUST', "Inserting into")

    def task_insert_abb_cust_idv_info(self, sql_file_path):
        self.run_abb_bq_queries(sql_file_path, 'cust_idv_info', "Inserting into")

    def run_abb_bq_queries(self, sql_file_path, table_name, operation):
        sql = commons.read_sql_file(sql_file_path).replace('{DEPLOY_ENV}', DEPLOY_ENV)
        run_bq_dml_with_log(f'{operation} {table_name} table', f'Completed {operation} {table_name} table', sql)
