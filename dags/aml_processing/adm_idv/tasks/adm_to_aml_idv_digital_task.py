
import logging
from aml_processing.adm_idv.tasks.adm_to_aml_idv_task import AdmToAmlIdvTask
from aml_processing.adm_idv.adm_idv_common import ADMIdvConst
from aml_processing.transaction.bq_util import run_bq_select, run_bq_dml_with_log


class AdmToAmlIdvDigitalTask(AdmToAmlIdvTask):

    def __init__(self, dag_run_id: str):
        super().__init__(dag_run_id)

    def execute(self):
        logging.info('Starting ADM Digital IDV Task.')
        self._insert_digital_idv()
        logging.info('Completed ADM Digital IDV Task.')

    def _get_last_process_time(self):
        sql = f"""
            SELECT MAX(LAST_PROCESS_TIME) AS DELTA_TS
            FROM {ADMIdvConst.TBL_ADM_DIGITAL_CTL}
        """

        result = run_bq_select(sql)
        max_last_process_time = next(result).get("DELTA_TS")
        logging.info(f"Latest process time: {max_last_process_time}")
        return max_last_process_time

    def _insert_digital_idv(self):
        delta_ts = self._get_last_process_time()

        # Handle None case - use default date matching the initialization in create_control_table_task
        # When delta_ts is None, the control table is empty, so we use the initial default date
        if delta_ts is None:
            delta_ts_filter = "PARSE_DATETIME('%Y-%m-%d', '1971-01-01')"
        else:
            # Format the datetime properly for SQL
            # BigQuery returns datetime values that can be converted to string
            delta_ts_str = str(delta_ts)
            delta_ts_filter = f"'{delta_ts_str}'"

        sql = f"""
            INSERT INTO {ADMIdvConst.TBL_AGG_AML_IDV_INFO}
            (
                CUSTOMER_NUMBER,
                APP_NUM,
                ID_TYPE,
                ID_TYPE_DESCRIPTION,
                ID_STATUS,
                ID_NUMBER,
                ID_STATE,
                ID_COUNTRY,
                ID_ISSUE_DATE,
                ID_EXPIRY_DATE,
                NAME_ON_SOURCE,
                REF_NUMBER,
                DATE_VERIFIED,
                TYPE_OF_INFO,
                IDV_METHOD,
                IDV_DECISION,
                REC_CREATE_TMS,
                RUN_ID
            )
            SELECT
                CUSTOMER_NUMBER,
                APP_NUM,
                ID_TYPE,
                ID_TYPE_DESCRIPTION,
                ID_STATUS,
                ID_NUMBER,
                ID_STATE,
                ID_COUNTRY,
                ID_ISSUE_DATE,
                ID_EXPIRY_DATE,
                SAFE_CAST(NAME_ON_SOURCE AS STRING),
                SAFE_CAST(REF_NUMBER AS STRING),
                SAFE_CAST(DATE_VERIFIED AS STRING),
                SAFE_CAST(TYPE_OF_INFO AS STRING),
                IDV_METHOD,
                IDV_DECISION,
                REC_CREATE_TMS,
                RUN_ID
            FROM
                (
                    SELECT
                        CUSTOMER_NUMBER,
                        APPLICANT_IDENTIFIER_UID                            AS APP_NUM,
                        ID_TYPE,
                        ID_TYPE                                             AS ID_TYPE_DESCRIPTION,
                        'PRIMARY'                                           AS ID_STATUS,
                        ID_NUMBER,
                        JURISDICTION                                        AS ID_STATE,
                        NATIONALITY                                         AS ID_COUNTRY,
                        SAFE_CAST(ISSUE_DATE AS STRING)                     AS ID_ISSUE_DATE,
                        SAFE_CAST(EXPIRY_DATE AS STRING)                    AS ID_EXPIRY_DATE,
                        NULL                                                AS NAME_ON_SOURCE,
                        NULL                                                AS REF_NUMBER,
                        NULL                                                AS DATE_VERIFIED,
                        NULL                                                AS TYPE_OF_INFO,
                        IDV_METHOD,
                        IDV_DECISION,
                        CURRENT_DATETIME()                                  AS REC_CREATE_TMS,
                        '{self.dag_run_id}'                                 AS RUN_ID
                    FROM
                        (
                            SELECT
                                api.CUSTOMER_NO AS CUSTOMER_NUMBER,
                                api.APPLICANT_IDENTIFIER_UID,
                                api.APPLICANT_IDENTIFIER_TYPE AS ID_TYPE,
                                api.APPLICANT_IDENTIFIER_VALUE AS ID_NUMBER,
                                api.JURISDICTION,
                                api.NATIONALITY,
                                api.ISSUE_DATE,
                                api.EXPIRY_DATE,
                                'DIDV' AS IDV_METHOD,
                                'DIDV' AS IDV_DECISION,
                                api.UPDATE_DT,
                                RANK() OVER (
                                                PARTITION BY
                                                    CUSTOMER_NO,
                                                    APPLICANT_IDENTIFIER_UID
                                                ORDER BY
                                                    UPDATE_DT DESC,
                                                    CREATE_DT DESC
                                            ) IDV_RANK
                            FROM
                                {ADMIdvConst.TBL_LANDING_APPLICANT_PERSONAL_IDENTIFIER}  api
                            WHERE
                                api.CUSTOMER_NO IS NOT NULL
                                AND api.APPLICANT_IDENTIFIER_VALUE IS NOT NULL and api.APPLICANT_IDENTIFIER_VALUE != ''
                                AND api.UPDATE_DT > {delta_ts_filter}
                        )
                    WHERE
                        IDV_RANK = 1
                )stg_didv
            WHERE
                NOT EXISTS (
                                SELECT
                                    1
                                FROM
                                    {ADMIdvConst.TBL_AGG_AML_IDV_INFO} a_idv
                                WHERE
                                    a_idv.CUSTOMER_NUMBER = stg_didv.CUSTOMER_NUMBER
                                    AND a_idv.APP_NUM = stg_didv.APP_NUM
                            )
        """

        run_bq_dml_with_log(
            f"Inserting into {ADMIdvConst.TBL_AGG_AML_IDV_INFO} for Digital IDV",
            f"Completed inserting into {ADMIdvConst.TBL_AGG_AML_IDV_INFO} for Digital IDV",
            sql
        )
