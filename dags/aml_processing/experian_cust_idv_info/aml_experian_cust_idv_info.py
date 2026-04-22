from util.miscutils import read_variable_or_file
from util.bq_utils import create_or_replace_table
from aml_processing.transaction.bq_util import run_bq_dml_with_log
from airflow.models.dagrun import DagRun
from aml_processing import feed_commons as commons
import logging

# CONSTANTS
DEPLOY_ENV = read_variable_or_file('gcp_config')['deployment_environment_name']

# TABLES
TBL_STG_EXP_BASE = f'pcb-{DEPLOY_ENV}-processing.domain_aml.EXP_BASE'
TBL_STG_EXP_ACC_CUST = f'pcb-{DEPLOY_ENV}-processing.domain_aml.EXP_ACC_CUST'
TBL_CURATED_AML_IDV_EXPERIAN_DATA = f'pcb-{DEPLOY_ENV}-curated.domain_aml.AML_IDV_EXPERIAN_DATA'
TBL_CURATED_CUST_IDV_INFO = f'pcb-{DEPLOY_ENV}-curated.domain_aml.CUST_IDV_INFO'


class ExperianCustIdvInfo:

    def task_create_exp_base_table(self, sql_file_path):
        sql = commons.read_sql_file(sql_file_path).replace('{DEPLOY_ENV}', DEPLOY_ENV)
        run_bq_dml_with_log(f'Creating {TBL_STG_EXP_BASE} table.', f'Completed creating {TBL_STG_EXP_BASE} table.', sql)

    def task_create_exp_acc_cust_table(self, sql_file_path):
        sql = commons.read_sql_file(sql_file_path).replace('{DEPLOY_ENV}', DEPLOY_ENV)
        run_bq_dml_with_log(f'Creating {TBL_STG_EXP_ACC_CUST} table.', f'Completed creating {TBL_STG_EXP_ACC_CUST} table.', sql)

    def task_create_aml_idv_experian_data(self, schema_file_path):
        logger = logging.getLogger("myLogger")
        logger.info(f'Creating {TBL_CURATED_AML_IDV_EXPERIAN_DATA} table.')
        create_or_replace_table(TBL_CURATED_AML_IDV_EXPERIAN_DATA, schema_file_path)
        logger.info(f'Completed creating {TBL_CURATED_AML_IDV_EXPERIAN_DATA} table.')

    def task_insert_into_aml_idv_experian_data(self, sql_file_path, **context):
        dr: DagRun = context["dag_run"]
        run_id = dr.run_id
        sql = commons.read_sql_file(sql_file_path).replace('{DEPLOY_ENV}', DEPLOY_ENV)
        sql_query = sql.replace('{run_id}', str(run_id))
        run_bq_dml_with_log(f'Inserting into {TBL_CURATED_AML_IDV_EXPERIAN_DATA} table.',
                            f'Completed inserting into {TBL_CURATED_AML_IDV_EXPERIAN_DATA} table.', sql_query)

    def task_delete_existing_experian_data_from_cust_idv_info(self, sql_file_path):
        sql = commons.read_sql_file(sql_file_path).replace('{DEPLOY_ENV}', DEPLOY_ENV)
        run_bq_dml_with_log(f'Deleting existing experian data from {TBL_CURATED_CUST_IDV_INFO} table.', f'Completed deleting existing experian data from {TBL_CURATED_CUST_IDV_INFO} table.', sql)

    def task_insert_into_cust_idv_info(self, sql_file_path):
        sql = commons.read_sql_file(sql_file_path).replace('{DEPLOY_ENV}', DEPLOY_ENV)
        run_bq_dml_with_log(f'Inserting into {TBL_CURATED_CUST_IDV_INFO} table.', f'Completed inserting into {TBL_CURATED_CUST_IDV_INFO} table.', sql)
