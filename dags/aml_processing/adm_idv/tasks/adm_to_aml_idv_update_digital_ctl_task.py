
from aml_processing.adm_idv.tasks.adm_to_aml_idv_task import AdmToAmlIdvTask
from aml_processing.adm_idv.adm_idv_common import ADMIdvConst
from aml_processing.transaction.bq_util import run_bq_dml_with_log


class AdmToAmlIdvUpdateDigitalCtlTask(AdmToAmlIdvTask):

    def __init__(self, dag_run_id: str):
        super().__init__(dag_run_id)

    def execute(self):
        run_bq_dml_with_log('Updating adm digital control table process time',
                            'Completed updating adm digital control table process time',
                            self.get_update_adm_digital_ctl_process_time_sql())

    def get_update_adm_digital_ctl_process_time_sql(self) -> str:
        # Update the control table record. If no record exists, insert one.
        # This ensures the table always has a record even if initialization failed or table was cleared.
        # Match on RUN_ID - if no rows exist, WHEN NOT MATCHED will insert
        sql = f"""MERGE `{ADMIdvConst.TBL_ADM_DIGITAL_CTL}` AS target
                  USING (
                      SELECT '{self.dag_run_id}' AS RUN_ID,
                             COALESCE((
                                 SELECT MAX(UPDATE_DT)
                                 FROM `{ADMIdvConst.TBL_LANDING_APPLICANT_PERSONAL_IDENTIFIER}`
                                 WHERE CUSTOMER_NO IS NOT NULL
                                    AND APPLICANT_IDENTIFIER_VALUE IS NOT NULL
                             ), PARSE_DATETIME('%Y-%m-%d', '1971-01-01')) AS LAST_PROCESS_TIME
                  ) AS source
                  ON target.RUN_ID = source.RUN_ID OR target.RUN_ID IS NOT NULL
                  WHEN MATCHED THEN
                      UPDATE SET RUN_ID = source.RUN_ID,
                                 LAST_PROCESS_TIME = source.LAST_PROCESS_TIME
                  WHEN NOT MATCHED THEN
                      INSERT (RUN_ID, LAST_PROCESS_TIME)
                      VALUES (source.RUN_ID, source.LAST_PROCESS_TIME)
                 """
        return sql
