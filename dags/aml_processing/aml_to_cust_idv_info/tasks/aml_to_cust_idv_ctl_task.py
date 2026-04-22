import logging
from aml_processing.transaction.bq_util import run_bq_dml_with_log
from aml_processing.aml_to_cust_idv_info.tasks.aml_to_cust_idv_task import AmlToCustIdvTask
from aml_processing.aml_to_cust_idv_info.aml_to_cust_idv_common import AmlCustIdvConst


class AmlToCustIdvCtlTask(AmlToCustIdvTask):
    def __init__(self, dag_run_id: str):
        super().__init__(dag_run_id)

    def execute(self):
        logging.info('Started Creating cust_idv_info_ctl control table task')
        run_bq_dml_with_log('Creating cust_idv_info_ctl control table',
                            'Completed creating cust_idv_info_ctl control table', self.create_cust_idv_info_ctl_table())
        run_bq_dml_with_log('Creating cust_idv_info_ctl control initial record if needed',
                            'Completed creating cust_idv_info_ctl control initial record', self.insert_into_cust_idv_info_ctl_table())
        logging.info('Completed Creating cust_idv_info_ctl control table task')

    def create_cust_idv_info_ctl_table(self) -> str:
        sql = f""" CREATE TABLE IF NOT EXISTS `{AmlCustIdvConst.TBL_CUST_CTL}`
                    (
                        RUN_ID                  STRING,
                        LAST_REC_CREATE_TMS     DATETIME
                    )
                """
        return sql

    def insert_into_cust_idv_info_ctl_table(self) -> str:
        # Create an initial record for the first run, each run would update the record with its run_id and run timestamp.
        sql = f"""INSERT INTO `{AmlCustIdvConst.TBL_CUST_CTL}`
                    (
                        RUN_ID,
                        LAST_REC_CREATE_TMS
                    )
                    SELECT
                        '{self.dag_run_id}',
                        PARSE_DATETIME('%Y-%m-%d', '1971-01-01')
                    FROM
                        (
                            SELECT 1
                        )
                    WHERE
                        NOT EXISTS
                            (
                                SELECT
                                    1
                                FROM
                                    `{AmlCustIdvConst.TBL_CUST_CTL}`
                            )
                """
        return sql
