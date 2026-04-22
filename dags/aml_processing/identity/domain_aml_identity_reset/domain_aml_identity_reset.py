from aml_processing.transaction.bq_util import run_bq_dml_with_log
from aml_processing.adm_idv.adm_idv_common import ADMIdvConst
from aml_processing import feed_commons as commons


class AMLIdentityReset():
    def task_adm_ctl_table_reset(self, sql_file_path):
        sql = commons.read_sql_file(sql_file_path).replace('{table_name}', ADMIdvConst.TBL_ADM_CTL)
        run_bq_dml_with_log(f'Purging {ADMIdvConst.TBL_ADM_CTL} table.', f'Completed purging {ADMIdvConst.TBL_ADM_CTL} table.', sql)

    def task_adm_digital_ctl_table_reset(self, sql_file_path):
        sql = commons.read_sql_file(sql_file_path).replace('{table_name}', ADMIdvConst.TBL_ADM_DIGITAL_CTL)
        run_bq_dml_with_log(f'Purging {ADMIdvConst.TBL_ADM_DIGITAL_CTL} table.', f'Completed purging {ADMIdvConst.TBL_ADM_DIGITAL_CTL} table.', sql)

    def task_aml_idv_excp_table_reset(self, sql_file_path):
        sql = commons.read_sql_file(sql_file_path).replace('{table_name}', ADMIdvConst.TBL_AML_IDV_EXCP)
        run_bq_dml_with_log(f'Purging {ADMIdvConst.TBL_AML_IDV_EXCP} table.', f'Completed purging {ADMIdvConst.TBL_AML_IDV_EXCP} table.', sql)

    def task_aml_idv_info_table_reset(self, sql_file_path):
        sql = commons.read_sql_file(sql_file_path).replace('{table_name}', ADMIdvConst.TBL_CURATED_AML_IDV_INFO)
        run_bq_dml_with_log(f'Purging {ADMIdvConst.TBL_CURATED_AML_IDV_INFO} table.', f'Completed purging {ADMIdvConst.TBL_CURATED_AML_IDV_INFO} table.', sql)

    def task_aml_cust_idv_info_table_reset(self, sql_file_path):
        sql = commons.read_sql_file(sql_file_path).replace('{table_name}', ADMIdvConst.TBL_AGG_CUST_IDV_INFO)
        run_bq_dml_with_log(f'Purging {ADMIdvConst.TBL_AGG_CUST_IDV_INFO} table.', f'Completed purging {ADMIdvConst.TBL_AGG_CUST_IDV_INFO} table.', sql)
