
from aml_processing.adm_idv.tasks.adm_to_aml_idv_task import AdmToAmlIdvTask
from aml_processing.adm_idv.adm_idv_common import ADMIdvConst
from aml_processing.transaction.bq_util import run_bq_dml_with_log


class AdmToAmlIdvUpdateCtlCompleteTask(AdmToAmlIdvTask):

    def __init__(self, dag_run_id: str):
        super().__init__(dag_run_id)

    def execute(self):
        run_bq_dml_with_log('Updating adm control table to completed',
                            'Completed updating adm control table status completed',
                            self.get_update_status_sql())

    def get_update_status_sql(self) -> str:
        sql = f"""UPDATE `{ADMIdvConst.TBL_ADM_CTL}` C
                     SET C.STATUS = '{ADMIdvConst.CTL_STATUS_COMPLETED}'
                  WHERE C.RUN_ID = '{self.dag_run_id}'
                """
        return sql
