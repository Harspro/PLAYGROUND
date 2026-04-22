from aml_processing.transaction.bq_util import run_bq_dml_with_log
from aml_processing.aml_to_cust_idv_info.tasks.aml_to_cust_idv_task import AmlToCustIdvTask
from aml_processing.aml_to_cust_idv_info.aml_to_cust_idv_common import AmlCustIdvConst
import logging


class AmlToCustIdvUpdateCtlCompleteTask(AmlToCustIdvTask):
    def __init__(self, dag_run_id: str):
        super().__init__(dag_run_id)

    def execute(self):
        logging.info('Starting the Control Table Update Task')
        run_bq_dml_with_log('Updating AML Customer IDV control table process time',
                            'Completed updating AML Customer IDV control table process time',
                            self.update_aml_cust_idv_ctl_table_process_time_sql())
        logging.info('Finished the Control Table Update Task')

    def update_aml_cust_idv_ctl_table_process_time_sql(self) -> str:
        sql = f"""UPDATE `{AmlCustIdvConst.TBL_CUST_CTL}`
                        SET RUN_ID = '{self.dag_run_id}',
                        LAST_REC_CREATE_TMS = (
                                                SELECT
                                                    MAX(REC_CREATE_TMS)
                                                FROM
                                                    `{AmlCustIdvConst.AML_IDV_INFO_TABLE}`
                                                WHERE
                                                    CUSTOMER_NUMBER IS NOT NULL
                                            )
                    WHERE 1 = 1
                 """
        return sql
