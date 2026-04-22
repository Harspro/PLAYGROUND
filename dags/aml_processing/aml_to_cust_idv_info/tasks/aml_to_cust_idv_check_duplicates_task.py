from aml_processing.aml_to_cust_idv_info.tasks.aml_to_cust_idv_task import AmlToCustIdvTask
from aml_processing.aml_to_cust_idv_info.aml_to_cust_idv_common import AmlCustIdvConst
from airflow.exceptions import AirflowFailException
import logging
from google.cloud import bigquery
from util.bq_utils import run_bq_query

logger = logging.getLogger(__name__)


class AmlToCustIdvCheckDuplicatesTask(AmlToCustIdvTask):

    def __init__(self, dag_run_id: str):
        super().__init__(dag_run_id)

    def execute(self):
        logger.info('Started Check for Duplicates task')
        # Checking the AML_IDV_INFO table for duplicates
        self.check_for_duplicates_in_source_table()
        logger.info('Completed  ADM FTF task')

    def check_for_duplicates_in_source_table(self):
        sql = f"""
                    SELECT
                        COUNT(1)
                    FROM
                        `{AmlCustIdvConst.AML_IDV_INFO_TABLE}`
                    GROUP BY
                        CUSTOMER_NUMBER,
                        APP_NUM
                    HAVING
                        COUNT(1) > 1
                """
        logger.info(f"sql satement: {sql}")
        query_result = run_bq_query(sql)
        row_count = query_result.result().total_rows
        logger.info(f"Duplicate check count: {row_count}")
        if (row_count != 0):
            logger.error(
                f"DAG failed due to duplicate customer numbers and app numbers in the {AmlCustIdvConst.AML_IDV_INFO_TABLE} table.")
            raise AirflowFailException(
                f"Duplicate Customer Numbers and App Numbers found in the {AmlCustIdvConst.AML_IDV_INFO_TABLE} table.")
        else:
            pass
