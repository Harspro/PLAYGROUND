import logging

from aml_processing.adm_idv.tasks.adm_to_aml_idv_nftf_pav_task import AdmToAmlIdvNFTFTPavTask
from aml_processing.adm_idv.tasks.adm_to_aml_idv_task import AdmToAmlIdvTask
from aml_processing.adm_idv.adm_idv_common import ADMIdvConst
from aml_processing.transaction.bq_util import run_bq_dml_with_log


TBL_STG_ADM_NFTF_SS = f"pcb-{ADMIdvConst.DEPLOY_ENV}-processing.domain_aml.STG_ADM_NFTF_SS"
TBL_STG_ADM_NFTF_SS_METHOD = f"pcb-{ADMIdvConst.DEPLOY_ENV}-processing.domain_aml.STG_ADM_NFTF_SS_METHOD"
TBL_STG_AML_IDV_INFO_SS = f"pcb-{ADMIdvConst.DEPLOY_ENV}-processing.domain_aml.STG_AML_IDV_INFO_SS"
TBL_LANDING_CIF_ACCOUNT_CURR = f"pcb-{ADMIdvConst.DEPLOY_ENV}-landing.domain_account_management.CIF_ACCOUNT_CURR"
TBL_LANDING_CIF_CARD_CURR = f"pcb-{ADMIdvConst.DEPLOY_ENV}-landing.domain_account_management.CIF_CARD_CURR"
TBL_STG_ADM_NFTF_SS_REFN_VDATE = f"pcb-{ADMIdvConst.DEPLOY_ENV}-processing.domain_aml.STG_ADM_NFTF_SS_REFN_VDATE"
TBL_STG_ADM_NFTF_SS_REFN_VDATE_ACCT = f"pcb-{ADMIdvConst.DEPLOY_ENV}-processing.domain_aml.STG_ADM_NFTF_SS_REFN_VDATE_ACCT"


class AdmToAmlIdvNFTFTSingleSourceTask(AdmToAmlIdvTask):

    def __init__(self, dag_run_id: str):
        super().__init__(dag_run_id)

    def execute(self):
        logging.info('Started ADM NFTF Single Source task...')
        self._3_1_create_single_source_stg()
        self._3_2_3_3_idv_method()
        self._3_4_3_6_create_stg_idv_refnum_dateverify()
        self._3_7_create_stg_adm_nftf_ss_refn_datev_acct()
        self._3_8_create_stg_adm_nftf_ss_final()
        logging.info('Completed ADM NFTF Single Source task...')

    def _3_1_create_single_source_stg(self):
        sql = f""" --3.1
                  CREATE OR REPLACE TABLE `{TBL_STG_ADM_NFTF_SS}` AS
                    SELECT
                    A.FILE_CREATE_DT,
                    A.APA_APP_NUM,
                    BU.BU_IND,
                    A.EXECUTION_ID,
                    CBM.CUSTOM_SS,
                    CBM.BUREAU_SS,
                    CBM.MANUAL_SS
                    FROM `{ADMIdvConst.TBL_STG_ADM_APP_NFTF}` a
                        INNER JOIN ( select
                                            ELE_ALPHA_VALUE as BU_IND,
                                            APA_APP_NUM,
                                            EXECUTION_ID,
                                            FILE_CREATE_DT,
                                            row_number() OVER(PARTITION BY FILE_CREATE_DT,EXECUTION_ID,APA_APP_NUM ORDER BY  REC_CHANGE_TMS DESC) REC_RANK
                                      FROM `{ADMIdvConst.TBL_LANDING_ADM_STD_DISP_DT_ELMT}`
                                      WHERE  ELE_ELEMENT_NAME='BUREAU_SELECTED'
                             ) BU ON BU.EXECUTION_ID = a.EXECUTION_ID
                                   and BU.FILE_CREATE_DT = a.FILE_CREATE_DT
                                   and BU.APA_APP_NUM = a.APA_APP_NUM
                                   and  BU.REC_RANK = 1
                          INNER JOIN  ( select
                                               APA_APP_NUM,
                                               EXECUTION_ID,
                                               FILE_CREATE_DT,
                                               MAX(IF(ELE_ELEMENT_NAME = "CUSTOM_SS", ELE_ALPHA_VALUE, NULL)) AS CUSTOM_SS,
                                               MAX(IF(ELE_ELEMENT_NAME = "BUREAU_SS", ELE_ALPHA_VALUE, NULL)) AS BUREAU_SS,
                                               MAX(IF(ELE_ELEMENT_NAME = "MANUAL_SS", ELE_ALPHA_VALUE, NULL)) AS MANUAL_SS,
                                               row_number() OVER(PARTITION BY FILE_CREATE_DT,EXECUTION_ID,APA_APP_NUM ORDER BY REC_CHANGE_TMS DESC) REC_RANK
                                      FROM `{ADMIdvConst.TBL_LANDING_ADM_STD_DISP_DT_ELMT}`
                                      WHERE  ELE_ELEMENT_NAME IN ('CUSTOM_SS','BUREAU_SS','MANUAL_SS')
                                           AND ELE_ALPHA_VALUE = 'Y'
                                      GROUP BY APA_APP_NUM,EXECUTION_ID,FILE_CREATE_DT,REC_CHANGE_TMS
                               ) CBM ON CBM.EXECUTION_ID = a.EXECUTION_ID
                                   and CBM.FILE_CREATE_DT = a.FILE_CREATE_DT
                                   and CBM.APA_APP_NUM = a.APA_APP_NUM
                                   and  CBM.REC_RANK = 1
                  WHERE NOT EXISTS ( SELECT 1
                                    FROM `{AdmToAmlIdvNFTFTPavTask.TBL_STG_APP_PAV_ID}` P
                                    WHERE P.APP_NUM = A.APA_APP_NUM
                                   )
        """
        run_bq_dml_with_log(f"3.1 Creating staging table:{TBL_STG_ADM_NFTF_SS}",
                            f"3.1  Completed creating staging table:{TBL_STG_ADM_NFTF_SS}",
                            sql
                            )

    def _3_2_3_3_idv_method(self):
        sql = f""" --3.2-3.3
                  CREATE OR REPLACE TABLE `{TBL_STG_ADM_NFTF_SS_METHOD}` AS
                    SELECT
                    FILE_CREATE_DT,
                    EXECUTION_ID,
                    APA_APP_NUM,
                    BUREAU,
                    FTF_IND,
                    IDV_DECISION,
                    IDV_METHOD
                    FROM (
                             SELECT   ss.FILE_CREATE_DT,
                                      ss.EXECUTION_ID,
                                      ss.APA_APP_NUM,
                                      (CASE WHEN BU_IND='MN' THEN 'EQ'
                                         WHEN BU_IND='HA' THEN 'TU'
                                         ELSE 'N/A' END) as BUREAU,
                                      'NFTF' as FTF_IND,
                                      (case
                                         WHEN BUREAU_SS = 'Y' THEN 'BUREAU_SS'
                                         WHEN CUSTOM_SS = 'Y' THEN 'CUSTOM_SS'
                                         WHEN MANUAL_SS = 'Y' THEN 'MANUAL_SS'
                                         ELSE 'NA'
                                         END) as IDV_DECISION
                                         ,
                                         'Credit File - Single Source' as IDV_METHOD,
                                         GEN_VAL.MOF
                             FROM `{TBL_STG_ADM_NFTF_SS}` ss
                                   INNER JOIN (select
                                                 --GEN_VAL_DETAIL_V.FILE_CREATE_DT,
                                                  --GEN_VAL_DETAIL_V.EXECUTION_ID,
                                                  GEN_VAL_DETAIL_V.APP_NUM,
                                                  MAX(GEN_VAL_DETAIL_V.NUMERIC_VALUE)/100000 as MOF
                                               FROM `{ADMIdvConst.TBL_STG_GEN_VAL_DETAIL_NFTF}` GEN_VAL_DETAIL_V
                                               WHERE GEN_VAL_DETAIL_V.ELEMENT_NAME in ('MONTHS ON FILE')
                                               GROUP BY  GEN_VAL_DETAIL_V.APP_NUM
                                            ) GEN_VAL ON  GEN_VAL.APP_NUM = ss.APA_APP_NUM
                           ) x
                     WHERE IDV_DECISION <> 'NA' AND x.MOF  >= 36"""
        run_bq_dml_with_log(f"3.2-3.3 Creating staging table:{TBL_STG_ADM_NFTF_SS_METHOD}",
                            f"3.2-3.3 Completed creating staging table:{TBL_STG_ADM_NFTF_SS_METHOD}",
                            sql
                            )

    def _3_4_3_6_create_stg_idv_refnum_dateverify(self):
        sql = f"""
                create or replace table `{TBL_STG_ADM_NFTF_SS_REFN_VDATE}` AS
                -- Manual SS 1
                SELECT  ADM_NFTF_SS_METHOD.FILE_CREATE_DT,
                        ADM_NFTF_SS_METHOD.EXECUTION_ID,
                        ADM_NFTF_SS_METHOD.APA_APP_NUM,
                        ADM_NFTF_SS_METHOD.BUREAU as ID_TYPE_DESCRIPTION,
                        ADM_NFTF_SS_METHOD.bureau as NAME_ON_SOURCE,
                        d_ref_num.REF_NUMBER as REF_NUMBER,
                        d_verify_date.DATE_VERIFY as DATE_VERIFIED,
                        ADM_NFTF_SS_METHOD.IDV_DECISION,
                        ADM_NFTF_SS_METHOD.IDV_METHOD
                    FROM `{TBL_STG_ADM_NFTF_SS_METHOD}` as ADM_NFTF_SS_METHOD
                              LEFT JOIN (SELECT  APP_NUM,
                                                  GVL_DATE DATE_VERIFY,
                                                  row_number() OVER(PARTITION BY APP_NUM ORDER BY FILE_CREATE_DT DESC,REC_CHNG_TMS DESC) REC_RANK
                                            FROM `{ADMIdvConst.TBL_STG_GEN_VAL_DETAIL_NFTF}` as GEN_VAL_DETAIL
                                            WHERE  ELEMENT_NAME in ('RETURN DATE')
                                                AND COMPONENT_NAME = 'MANUAL'
                                                AND GVL_PRODUCT = 'MN'
                                          ) d_verify_date on d_verify_date.APP_NUM      = ADM_NFTF_SS_METHOD.APA_APP_NUM
                                                        and d_verify_date.REC_RANK = 1
                              INNER JOIN (SELECT FILE_CREATE_DT,
                                                  APP_NUM,
                                                  ALPHA_VALUE REF_NUMBER,
                                                  row_number() OVER(PARTITION BY APP_NUM ORDER BY FILE_CREATE_DT DESC, REC_CHNG_TMS DESC) REC_RANK
                                            FROM `{ADMIdvConst.TBL_STG_GEN_VAL_DETAIL_NFTF}` as GEN_VAL_DETAIL
                                            WHERE  ELEMENT_NAME in ('H UNIQUE NUMBER')
                                                AND COMPONENT_NAME = 'MANUAL'
                                                AND GVL_PRODUCT = 'MN'
                                          ) d_ref_num on d_ref_num.APP_NUM = ADM_NFTF_SS_METHOD.APA_APP_NUM
                                                        and d_ref_num.REC_RANK = 1
                WHERE ADM_NFTF_SS_METHOD.BUREAU = 'EQ'
                and ADM_NFTF_SS_METHOD.IDV_DECISION = 'MANUAL_SS'
                UNION ALL
                -- Manual SS 2
                SELECT  ADM_NFTF_SS_METHOD.FILE_CREATE_DT,
                        ADM_NFTF_SS_METHOD.EXECUTION_ID,
                        ADM_NFTF_SS_METHOD.APA_APP_NUM,
                        ADM_NFTF_SS_METHOD.BUREAU as ID_TYPE_DESCRIPTION,
                        ADM_NFTF_SS_METHOD.bureau as NAME_ON_SOURCE,
                        d_ref_num.REF_NUMBER as REF_NUMBER,
                        d_verify_date.DATE_VERIFY as DATE_VERIFIED,
                        ADM_NFTF_SS_METHOD.IDV_DECISION,
                        ADM_NFTF_SS_METHOD.IDV_METHOD
                FROM `{TBL_STG_ADM_NFTF_SS_METHOD}` as ADM_NFTF_SS_METHOD
                              LEFT JOIN (SELECT APP_NUM,
                                                GVL_DATE DATE_VERIFY,
                                                  row_number() OVER(PARTITION BY APP_NUM ORDER BY FILE_CREATE_DT DESC, REC_CHNG_TMS DESC) REC_RANK
                                            FROM `{ADMIdvConst.TBL_STG_GEN_VAL_DETAIL_NFTF}` as GEN_VAL_DETAIL
                                            WHERE  ELEMENT_NAME in ('RETURN DATE')
                                                AND COMPONENT_NAME = 'MANUAL'
                                                AND GVL_PRODUCT = 'HA'
                                          ) d_verify_date on  d_verify_date.APP_NUM  = ADM_NFTF_SS_METHOD.APA_APP_NUM
                                                        and d_verify_date.REC_RANK = 1
                              INNER JOIN (SELECT FILE_CREATE_DT,
                                                  APP_NUM,
                                                  ALPHA_VALUE REF_NUMBER,
                                                  row_number() OVER(PARTITION BY APP_NUM ORDER BY  FILE_CREATE_DT DESC,REC_CHNG_TMS DESC) REC_RANK
                                            FROM `{ADMIdvConst.TBL_STG_GEN_VAL_DETAIL_NFTF}` as GEN_VAL_DETAIL
                                            WHERE  ELEMENT_NAME in ('CONSUMER FILE ID')
                                                AND COMPONENT_NAME = 'MANUAL'
                                                AND GVL_PRODUCT = 'HA'
                                          ) d_ref_num on d_ref_num.APP_NUM      = ADM_NFTF_SS_METHOD.APA_APP_NUM
                                                        and d_ref_num.REC_RANK = 1
                WHERE ADM_NFTF_SS_METHOD.BUREAU='TU'
                and ADM_NFTF_SS_METHOD.IDV_DECISION='MANUAL_SS'
                UNION ALL
                -- Custom SS AND BUREAU SS 1
                SELECT  ADM_NFTF_SS_METHOD.FILE_CREATE_DT,
                        ADM_NFTF_SS_METHOD.EXECUTION_ID,
                        ADM_NFTF_SS_METHOD.APA_APP_NUM,
                        ADM_NFTF_SS_METHOD.BUREAU as ID_TYPE_DESCRIPTION,
                        ADM_NFTF_SS_METHOD.bureau as NAME_ON_SOURCE,
                        d_ref_num.REF_NUMBER as REF_NUMBER,
                        d_verify_date.DATE_VERIFY as DATE_VERIFIED,
                        ADM_NFTF_SS_METHOD.IDV_DECISION,
                        ADM_NFTF_SS_METHOD.IDV_METHOD
                FROM `{TBL_STG_ADM_NFTF_SS_METHOD}` as ADM_NFTF_SS_METHOD
                              LEFT JOIN (SELECT  APP_NUM,
                                                  GVL_DATE DATE_VERIFY,
                                                  row_number() OVER(PARTITION BY APP_NUM ORDER BY FILE_CREATE_DT DESC,REC_CHNG_TMS DESC) REC_RANK
                                            FROM `{ADMIdvConst.TBL_STG_GEN_VAL_DETAIL_NFTF}` as GEN_VAL_DETAIL
                                            WHERE  ELEMENT_NAME in ('RETURN DATE')
                                                AND COMPONENT_NAME = 'CONSUMER'
                                                AND GVL_PRODUCT = 'MN'
                                          ) d_verify_date on  d_verify_date.APP_NUM = ADM_NFTF_SS_METHOD.APA_APP_NUM
                                                        and d_verify_date.REC_RANK = 1
                              INNER JOIN (SELECT  APP_NUM,
                                                  ALPHA_VALUE REF_NUMBER,
                                                  row_number() OVER(PARTITION BY APP_NUM ORDER BY FILE_CREATE_DT DESC, REC_CHNG_TMS DESC) REC_RANK
                                            FROM `{ADMIdvConst.TBL_STG_GEN_VAL_DETAIL_NFTF}` as GEN_VAL_DETAIL
                                            WHERE  ELEMENT_NAME in ('H UNIQUE NUMBER')
                                                AND COMPONENT_NAME = 'CONSUMER'
                                                AND GVL_PRODUCT = 'MN'
                                          ) d_ref_num on d_ref_num.APP_NUM      = ADM_NFTF_SS_METHOD.APA_APP_NUM
                                                        and d_ref_num.REC_RANK = 1
                WHERE ADM_NFTF_SS_METHOD.BUREAU='EQ'
                and ADM_NFTF_SS_METHOD.IDV_DECISION IN ('CUSTOM_SS','BUREAU_SS')
                UNION ALL
                -- custom and manual SS 2
                SELECT  ADM_NFTF_SS_METHOD.FILE_CREATE_DT,
                        ADM_NFTF_SS_METHOD.EXECUTION_ID,
                        ADM_NFTF_SS_METHOD.APA_APP_NUM,
                        ADM_NFTF_SS_METHOD.BUREAU as ID_TYPE_DESCRIPTION,
                        ADM_NFTF_SS_METHOD.bureau as NAME_ON_SOURCE,
                        d_ref_num.REF_NUMBER as REF_NUMBER,
                        d_verify_date.DATE_VERIFY as DATE_VERIFIED,
                        ADM_NFTF_SS_METHOD.IDV_DECISION,
                        ADM_NFTF_SS_METHOD.IDV_METHOD
                FROM `{TBL_STG_ADM_NFTF_SS_METHOD}` as ADM_NFTF_SS_METHOD
                              LEFT JOIN (SELECT  APP_NUM,
                                                  GVL_DATE DATE_VERIFY,
                                                  row_number() OVER(PARTITION BY APP_NUM ORDER BY FILE_CREATE_DT DESC,REC_CHNG_TMS DESC) REC_RANK
                                            FROM `{ADMIdvConst.TBL_STG_GEN_VAL_DETAIL_NFTF}` as GEN_VAL_DETAIL
                                            WHERE  ELEMENT_NAME in ('RETURN DATE')
                                                AND COMPONENT_NAME = 'CONSUMER'
                                                AND GVL_PRODUCT = 'HA'
                                          ) d_verify_date on  d_verify_date.APP_NUM      = ADM_NFTF_SS_METHOD.APA_APP_NUM
                                                        and d_verify_date.REC_RANK = 1
                              INNER JOIN (SELECT  APP_NUM,
                                                  ALPHA_VALUE REF_NUMBER,
                                                  row_number() OVER(PARTITION BY APP_NUM ORDER BY FILE_CREATE_DT DESC, REC_CHNG_TMS DESC) REC_RANK
                                            FROM `{ADMIdvConst.TBL_STG_GEN_VAL_DETAIL_NFTF}` as GEN_VAL_DETAIL
                                            WHERE  ELEMENT_NAME in ('CONSUMER FILE ID')
                                                AND COMPONENT_NAME = 'CONSUMER'
                                                AND GVL_PRODUCT = 'HA'
                                          ) d_ref_num on  d_ref_num.APP_NUM  = ADM_NFTF_SS_METHOD.APA_APP_NUM
                                                        and d_ref_num.REC_RANK = 1
                WHERE ADM_NFTF_SS_METHOD.BUREAU = 'TU'
                and ADM_NFTF_SS_METHOD.IDV_DECISION IN ('CUSTOM_SS','BUREAU_SS')
                """
        run_bq_dml_with_log(f'3.4-3.6 Creating {TBL_STG_ADM_NFTF_SS_REFN_VDATE}',
                            f'Completed creating{TBL_STG_ADM_NFTF_SS_REFN_VDATE}', sql)

    def _3_7_create_stg_adm_nftf_ss_refn_datev_acct(self):
        sql = f"""CREATE OR REPLACE TABLE `{TBL_STG_ADM_NFTF_SS_REFN_VDATE_ACCT}` AS
                        SELECT   M_SS.APA_APP_NUM,
                                 LTTR_OVRD_DETAIL_XTO.TS2_ACCOUNT_ID ACCOUNT_ID,
                                 M_SS.REF_NUMBER,
                                 M_SS.NAME_ON_SOURCE,
                                 M_SS.NAME_ON_SOURCE as ID_TYPE,
                                 M_SS,ID_TYPE_DESCRIPTION,
                                 M_SS.DATE_VERIFIED ,
                                 NULL as ISSUE_STATE,
                        '        CA' as ID_COUNTRY,
                                 M_SS.IDV_DECISION,
                                 M_SS.IDV_METHOD
                        FROM `{TBL_STG_ADM_NFTF_SS_REFN_VDATE}` M_SS
                                LEFT OUTER JOIN  (SELECT FILE_CREATE_DT,
                                                         APP_NUM,
                                                         TS2_ACCOUNT_ID,
                                                         row_number() OVER(PARTITION BY APP_NUM ORDER BY FILE_CREATE_DT DESC, REC_CHNG_TMS DESC) REC_RANK
                                                      FROM `{ADMIdvConst.TBL_LANDING_LTTR_OVRD_DETAIL_XTO}` LTTR_OVRD_DETAIL_XTO
                                                      WHERE  LTTR_OVRD_DETAIL_XTO.TS2_ACCOUNT_ID is not null
                                                          AND LTTR_OVRD_DETAIL_XTO.TS2_ACCOUNT_ID <> 0
                                                     ) LTTR_OVRD_DETAIL_XTO ON   LTTR_OVRD_DETAIL_XTO.APP_NUM = M_SS.APA_APP_NUM
                                                                                AND LTTR_OVRD_DETAIL_XTO.REC_RANK = 1
                 """
        run_bq_dml_with_log(f'3.7 Creating {TBL_STG_ADM_NFTF_SS_REFN_VDATE_ACCT}',
                            'Completed creating{TBL_STG_ADM_NFTF_SS_REFN_VDATE_ACCT}', sql)

    def _3_8_create_stg_adm_nftf_ss_final(self):
        sql = f"""
                    CREATE OR REPLACE TABLE `{ADMIdvConst.TBL_STG_ADM_NFTF_SS_FINAL}` AS
                    SELECT APA_APP_NUM              AS APP_NUM,
                             ACCOUNT_ID,
                             REF_NUMBER,
                             NAME_ON_SOURCE,
                             NAME_ON_SOURCE         AS ID_TYPE,
                             ID_TYPE_DESCRIPTION,
                             DATE_VERIFIED ,
                             ISSUE_STATE,
                              'CA'                  AS ID_COUNTRY,
                             IDV_DECISION,
                             IDV_METHOD
                    FROM `{TBL_STG_ADM_NFTF_SS_REFN_VDATE_ACCT}`
                    WHERE REF_NUMBER IS NOT NULL
                          AND REF_NUMBER != ''
                          AND ACCOUNT_ID IS NOT NULL
                    """
        run_bq_dml_with_log(f'3.8 Creating {ADMIdvConst.TBL_STG_ADM_NFTF_SS_FINAL}',
                            f'Completed creating{ADMIdvConst.TBL_STG_ADM_NFTF_SS_FINAL}', sql)
