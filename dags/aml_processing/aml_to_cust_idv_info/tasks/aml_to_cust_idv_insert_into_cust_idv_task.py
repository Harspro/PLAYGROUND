from aml_processing.transaction.bq_util import run_bq_select, run_bq_dml_with_log
from aml_processing.aml_to_cust_idv_info.tasks.aml_to_cust_idv_task import AmlToCustIdvTask
from aml_processing.aml_to_cust_idv_info.aml_to_cust_idv_common import AmlCustIdvConst
import logging


class AmlToCustIdvInsertIntoCustIdvTask(AmlToCustIdvTask):
    def __init__(self, dag_run_id: str):
        self.dag_run_id = dag_run_id

    def execute(self):
        logging.info('Starting the AML to CUST_IDV_INFO Insertion Task')
        # Inserting Data from AML_IDV_INFO to CUST_IDV_INFO table
        run_bq_dml_with_log(f"Inserting into {AmlCustIdvConst.AGG_CUST_IDV_INFO_TABLE} from :{AmlCustIdvConst.AML_IDV_INFO_TABLE}",
                            f"Completed inserting into {AmlCustIdvConst.AGG_CUST_IDV_INFO_TABLE} from staging table:{AmlCustIdvConst.AML_IDV_INFO_TABLE}", self.insert_into_cust_idv_info())
        logging.info('Finished the AML to CUST_IDV_INFO Insertion Task')

    def get_last_rec_create_tms(self):
        sql = f"""
                SELECT
                    MAX(LAST_REC_CREATE_TMS) AS DELTA_TS
                FROM
                    `{AmlCustIdvConst.TBL_CUST_CTL}`
        """

        result = run_bq_select(sql)
        max_last_rec_create_tms = next(result).get("DELTA_TS")
        logging.info(f"Latest process time: {max_last_rec_create_tms}")
        return max_last_rec_create_tms

    def insert_into_cust_idv_info(self) -> str:
        delta_ts = self.get_last_rec_create_tms()
        sql = f"""
                    INSERT INTO `{AmlCustIdvConst.AGG_CUST_IDV_INFO_TABLE}`
                    (
                        CUSTOMER_NUMBER,
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
                        CREATE_DT
                    )
                    SELECT
                        CUSTOMER_NUMBER,
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
                        CREATE_DT
                    FROM
                        (
                            SELECT
                                aml_idv.CUSTOMER_NUMBER,
                                aml_idv.ID_TYPE,
                                aml_idv.ID_TYPE_DESCRIPTION,
                                aml_idv.ID_STATUS,
                                aml_idv.ID_NUMBER,
                                aml_idv.ID_STATE,
                                aml_idv.ID_COUNTRY,
                                CASE
                                    WHEN
                                        aml_idv.IDV_METHOD IN ('FTF' , 'PAV')
                                    THEN
                                        SAFE.PARSE_DATE('%m%d%Y' , aml_idv.ID_ISSUE_DATE)
                                    WHEN
                                        aml_idv.IDV_METHOD = 'DIDV'
                                    THEN
                                        EXTRACT(DATE FROM SAFE_CAST(aml_idv.ID_ISSUE_DATE AS DATETIME))
                                    ELSE
                                        NULL
                                END                                                         AS ID_ISSUE_DATE,
                                CASE
                                    WHEN
                                        aml_idv.IDV_METHOD IN ('FTF' , 'PAV')
                                    THEN
                                        SAFE.PARSE_DATE('%m%d%Y' , aml_idv.ID_EXPIRY_DATE)
                                    WHEN
                                        aml_idv.IDV_METHOD = 'DIDV'
                                    THEN
                                        EXTRACT(DATE FROM SAFE_CAST(aml_idv.ID_EXPIRY_DATE AS DATETIME))
                                    ELSE
                                        NULL
                                END                                                         AS ID_EXPIRY_DATE,
                                aml_idv.NAME_ON_SOURCE,
                                aml_idv.REF_NUMBER,
                                SAFE.PARSE_DATE('%Y%m%d' , aml_idv.DATE_VERIFIED)           AS DATE_VERIFIED,
                                aml_idv.TYPE_OF_INFO,
                                aml_idv.IDV_METHOD,
                                aml_idv.IDV_DECISION,
                                CURRENT_DATETIME()                                          AS CREATE_DT,
                                ROW_NUMBER() OVER(
                                                    PARTITION BY
                                                        aml_idv.CUSTOMER_NUMBER
                                                    ORDER BY
                                                        CASE
                                                            WHEN
                                                                aml_idv.IDV_METHOD = 'FTF'
                                                            THEN
                                                                1
                                                            WHEN
                                                                aml_idv.IDV_METHOD = 'PAV'
                                                            THEN
                                                                2
                                                            WHEN
                                                                aml_idv.IDV_METHOD = 'DIDV'
                                                            THEN
                                                                3
                                                            WHEN
                                                                aml_idv.IDV_METHOD = 'Credit File - Single Source'
                                                            THEN
                                                                4
                                                            WHEN
                                                                aml_idv.IDV_METHOD IN ('Credit File - Dual Source','Dual Source')
                                                            THEN
                                                                5
                                                        END ASC
                                                ) AS REC_RANK
                            FROM
                                `{AmlCustIdvConst.AML_IDV_INFO_TABLE}` aml_idv
                            WHERE
                                aml_idv.CUSTOMER_NUMBER IS NOT NULL
                                AND aml_idv.REC_CREATE_TMS > '{delta_ts}'

                        ) aml_idv_filtered
                    WHERE
                        aml_idv_filtered.REC_RANK = 1
                        AND NOT EXISTS
                        (
                            SELECT
                                cust_idv.CUSTOMER_NUMBER
                            FROM
                                `{AmlCustIdvConst.AGG_CUST_IDV_INFO_TABLE}` cust_idv
                            WHERE
                                cust_idv.CUSTOMER_NUMBER = aml_idv_filtered.CUSTOMER_NUMBER
                        )
                """
        return sql
