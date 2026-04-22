from datetime import date
from typing import List
from google.cloud import bigquery
from aml_processing.adm_idv.adm_idv_common import ADMIdvConst
from aml_processing.transaction.bq_util import run_bq_select


class AdmToAmlIdvTask:

    def __init__(self, dag_run_id: str):
        self.dag_run_id = dag_run_id

    def get_file_create_dates(self) -> List[date]:
        sql = f"""
              SELECT  DISTINCT C.FILE_CREATE_DT
              FROM {ADMIdvConst.TBL_ADM_CTL} C
              WHERE C.RUN_ID = @dag_run_id
              AND C.LATEST_VERSION = 'Y'
            """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("dag_run_id", "STRING", self.dag_run_id)
            ]
        )
        file_create_date_list: List[date] = []
        result = run_bq_select(sql, job_config=job_config)
        for row in result:
            file_create_date_list.append(row['FILE_CREATE_DT'])
        return file_create_date_list

    def execute(self):
        pass
