from aml_processing.adm_idv.tasks.adm_to_aml_idv_task import AdmToAmlIdvTask
from aml_processing.adm_idv.adm_idv_common import ADMIdvConst
from aml_processing.transaction.bq_util import run_bq_dml_with_log

TBL_STG_ADM_NFTF_DS1 = f"pcb-{ADMIdvConst.DEPLOY_ENV}-processing.domain_aml.STG_ADM_NFTF_DS1"
TBL_STG_ADM_NFTF_DS2 = f"pcb-{ADMIdvConst.DEPLOY_ENV}-processing.domain_aml.STG_ADM_NFTF_DS2"
TBL_STG_ADM_NFTF_DS3 = f"pcb-{ADMIdvConst.DEPLOY_ENV}-processing.domain_aml.STG_ADM_NFTF_DS3"
TBL_STG_ADM_NFTF_DS4 = f"pcb-{ADMIdvConst.DEPLOY_ENV}-processing.domain_aml.STG_ADM_NFTF_DS4"
TBL_STG_GEN_VAL_ID = f"pcb-{ADMIdvConst.DEPLOY_ENV}-processing.domain_aml.STG_GEN_VAL_ID"
TBL_STG_ADM_NFTF_BU = f"pcb-{ADMIdvConst.DEPLOY_ENV}-processing.domain_aml.STG_ADM_NFTF_BU"

TBL_STG_ADM_NFTF_DS_TRADE = f"pcb-{ADMIdvConst.DEPLOY_ENV}-processing.domain_aml.STG_ADM_NFTF_TRADE"
TBL_STG_ADM_NFTF_DS_TUDS = f"pcb-{ADMIdvConst.DEPLOY_ENV}-processing.domain_aml.STG_ADM_NFTF_TUDS"
TBL_STG_ADM_NFTF_DS_MDS = f"pcb-{ADMIdvConst.DEPLOY_ENV}-processing.domain_aml.STG_ADM_NFTF_MDS"
TBL_STG_ADM_NFTF_DS_OTH = f"pcb-{ADMIdvConst.DEPLOY_ENV}-processing.domain_aml.STG_ADM_NFTF_DS_OTH"
TBL_STG_ADM_NFTF_DS_CHK = f"pcb-{ADMIdvConst.DEPLOY_ENV}-processing.domain_aml.STG_ADM_NFTF_DS_CHK"

TBL_STG_ADM_NFTF_DS_UNION = f"pcb-{ADMIdvConst.DEPLOY_ENV}-processing.domain_aml.STG_ADM_NFTF_DS_UNION"
TBL_STG_ADM_NFTF_DS_FINAL = f"pcb-{ADMIdvConst.DEPLOY_ENV}-processing.domain_aml.STG_ADM_NFTF_DS_FINAL"
TBL_STG_ADM_NFTF_DS_EXP = f"pcb-{ADMIdvConst.DEPLOY_ENV}-processing.domain_aml.STG_ADM_NFTF_DS_EXCP"


TBL_STG_ADM_STD_DISP_USER = f"pcb-{ADMIdvConst.DEPLOY_ENV}-processing.domain_aml.STG_ADM_STD_DISP_USER"


class AdmToAmlIdvNFTFTDualSourceTask(AdmToAmlIdvTask):

    def __init__(self, dag_run_id: str):
        super().__init__(dag_run_id)

    def execute(self):
        self._4_1_create_stg_adm_nftf_ds1()
        self._4_2_4_3_create_stg_adm_nftf_ds2_mof()
        self._4_4_create_stg_adm_nftf_ds3_mof()
        self._4_5_create_stg_adm_nftf_ds4_idv_decision()
        self._4_6_create_stg_gen_val_id()
        self._4_7_create_stg_adm_nftf_bu()
        self._4_8_create_stg_adm_nftf_trade()
        self._4_10_create_stg_adm_nftf_tuds()
        self._4_12_create_stg_adm_nftf_mds()
        self._4_14_pre_create_stg_adm_disp_user()
        self._4_14_create_stg_adm_nftf_ds_oth()
        self._4_16_create_stg_adm_nftf_ds_chk()
        self._4_21_1_create_stg_adm_nftf_ds_union()
        self._4_21_2_create_stg_adm_nftf_ds_final()
        self._4_21_3_create_stg_adm_nftf_ds_excp()

    def _4_1_create_stg_adm_nftf_ds1(self):
        sql = f"""  --4.1
                    CREATE OR REPLACE TABLE `{TBL_STG_ADM_NFTF_DS1}` as
                    SELECT
                       ADM_STD_DISP_DT_ELMT_V.APA_APP_NUM,
                       ADM_STD_DISP_DT_ELMT_V.ELE_ELEMENT_NAME,
                       ADM_STD_DISP_DT_ELMT_V.ELE_ALPHA_VALUE,
                       ADM_STD_DISP_DT_ELMT_V.EXECUTION_ID
                    FROM
                      `{ADMIdvConst.TBL_STG_ADM_APP_NFTF}` nftf
                       INNER JOIN (
                                    SELECT
                                            EXECUTION_ID,
                                            APA_APP_NUM,
                                            ELE_ELEMENT_NAME,
                                            ELE_ALPHA_VALUE
                                      FROM `{ADMIdvConst.TBL_STG_ADM_DT_ELMT}`
                                      WHERE ELE_ELEMENT_NAME IN ('NM_DOB_TRADELINE1','NM_ADDR_TRADELINE2',
                                                                'NM_DOB_TRADELINE2','NM_ADDR_TRADELINE1',
                                                                'NM_ADDR_TU_DS','NM_ADDR_CRA','NM_ADDR_MOBILE_DS',
                                                                'NM_ADDR_UTILITY','NM_ADDR_PPTY_TAX','NM_ACCT_CHEQ',
                                                                'NM_ACCT_BNK_STMT','NM_DOB_CRA',
                                                                'NM_ADDR_TU_DS','NM_DOB_MOBILE_DS')
                           )  ADM_STD_DISP_DT_ELMT_V
                            ON ADM_STD_DISP_DT_ELMT_V.APA_APP_NUM = nftf.APA_APP_NUM
                            AND ADM_STD_DISP_DT_ELMT_V.EXECUTION_ID = nftf.EXECUTION_ID
                    where NOT EXISTS (SELECT 1
                                      FROM `{ADMIdvConst.TBL_STG_ADM_NFTF_SS_FINAL}` ss
                                      WHERE  ss.APP_NUM = nftf.APA_APP_NUM)
          """
        run_bq_dml_with_log(f'4.1 Creating {TBL_STG_ADM_NFTF_DS1}',
                            f'Completed creating{TBL_STG_ADM_NFTF_DS1}', sql)

    def _4_2_4_3_create_stg_adm_nftf_ds2_mof(self):
        sql = f""" -- 4.2 - 4.3
                  CREATE OR REPLACE TABLE `{TBL_STG_ADM_NFTF_DS2}` as
                  SELECT
                       ds1.APA_APP_NUM,
                       ds1.ELE_ELEMENT_NAME,
                       ds1.ELE_ALPHA_VALUE
                   FROM `{TBL_STG_ADM_NFTF_DS1}` ds1
                   INNER JOIN (SELECT  GEN_VAL_DETAIL_V.APP_NUM,
                                       MAX(GEN_VAL_DETAIL_V.NUMERIC_VALUE) / 100000 as MOF
                               FROM `{ADMIdvConst.TBL_STG_GEN_VAL_DETAIL_NFTF}` GEN_VAL_DETAIL_V
                               WHERE GEN_VAL_DETAIL_V.ELEMENT_NAME in ('MONTHS ON FILE')
                               GROUP BY GEN_VAL_DETAIL_V.APP_NUM
                            ) gen_val_mof
                                 ON ds1.APA_APP_NUM = gen_val_mof.APP_NUM
                   WHERE gen_val_mof.MOF >= 6"""
        run_bq_dml_with_log(f'4.2_4.3 ßCreating {TBL_STG_ADM_NFTF_DS2}',
                            f'Completed creating{TBL_STG_ADM_NFTF_DS2}', sql)

    def _4_4_create_stg_adm_nftf_ds3_mof(self):
        sql = f""" --4.4
                 CREATE OR REPLACE TABLE `{TBL_STG_ADM_NFTF_DS3}` as
                 SELECT APA_APP_NUM
                        ,MAX(IF(ELE_ELEMENT_NAME = 'NM_DOB_TRADELINE1', ELE_ALPHA_VALUE, NULL)) as NM_DOB_TRADELINE1
                        ,MAX(IF(ELE_ELEMENT_NAME = 'NM_ADDR_TRADELINE2', ELE_ALPHA_VALUE, NULL)) as NM_ADDR_TRADELINE2
                        ,MAX(IF(ELE_ELEMENT_NAME = 'NM_DOB_TRADELINE2', ELE_ALPHA_VALUE, NULL)) as NM_DOB_TRADELINE2
                        ,MAX(IF(ELE_ELEMENT_NAME = 'NM_ADDR_TRADELINE1', ELE_ALPHA_VALUE, NULL)) as NM_ADDR_TRADELINE1
                        ,MAX(IF(ELE_ELEMENT_NAME = 'NM_ADDR_TU_DS', ELE_ALPHA_VALUE, NULL)) as NM_ADDR_TU_DS
                        ,MAX(IF(ELE_ELEMENT_NAME = 'NM_ADDR_CRA', ELE_ALPHA_VALUE, NULL)) as NM_ADDR_CRA
                        ,MAX(IF(ELE_ELEMENT_NAME = 'NM_ADDR_MOBILE_DS', ELE_ALPHA_VALUE, NULL)) as NM_ADDR_MOBILE_DS
                        ,MAX(IF(ELE_ELEMENT_NAME = 'NM_ADDR_UTILITY', ELE_ALPHA_VALUE, NULL)) as NM_ADDR_UTILITY
                        ,MAX(IF(ELE_ELEMENT_NAME = 'NM_ADDR_PPTY_TAX', ELE_ALPHA_VALUE, NULL)) as NM_ADDR_PPTY_TAX
                        ,MAX(IF(ELE_ELEMENT_NAME = 'NM_ACCT_CHEQ', ELE_ALPHA_VALUE, NULL)) as NM_ACCT_CHEQ
                        ,MAX(IF(ELE_ELEMENT_NAME = 'NM_ACCT_BNK_STMT', ELE_ALPHA_VALUE, NULL)) as NM_ACCT_BNK_STMT
                        ,MAX(IF(ELE_ELEMENT_NAME = 'NM_DOB_CRA', ELE_ALPHA_VALUE, NULL)) as NM_DOB_CRA
                        --,MAX(IF(ELE_ELEMENT_NAME = 'NM_ADDR_TU_DS', ELE_ALPHA_VALUE, NULL)) as NM_ADDR_TU_DS
                        ,MAX(IF(ELE_ELEMENT_NAME = 'NM_DOB_MOBILE_DS', ELE_ALPHA_VALUE, NULL)) as NM_DOB_MOBILE_DS
                 FROM `{TBL_STG_ADM_NFTF_DS2}`
                 GROUP BY APA_APP_NUM
        """
        run_bq_dml_with_log(f'4.4 Creating {TBL_STG_ADM_NFTF_DS3}',
                            f'Completed creating {TBL_STG_ADM_NFTF_DS3}', sql)

    def _4_5_create_stg_adm_nftf_ds4_idv_decision(self):
        sql = f""" --4.5
                CREATE OR REPLACE TABLE `{TBL_STG_ADM_NFTF_DS4}` as
                SELECT
                APA_APP_NUM,
                CASE  WHEN
                ((NM_DOB_TRADELINE1 = 'Y' and NM_ADDR_TRADELINE2  = 'Y') or
                         (NM_DOB_TRADELINE2 = 'Y' and NM_ADDR_TRADELINE1 = 'Y') OR
                         (NM_ADDR_TRADELINE1 = 'Y' and NM_DOB_MOBILE_DS = 'Y' ) OR
                         (NM_ADDR_TRADELINE1 = 'Y' and NM_ACCT_CHEQ = 'Y') OR
                         (NM_ADDR_TRADELINE1 = 'Y' and NM_ACCT_BNK_STMT = 'Y') OR
                         (NM_ADDR_TRADELINE1 = 'Y' and NM_DOB_CRA = 'Y') OR
                         (NM_ADDR_TRADELINE2 = 'Y' and NM_DOB_CRA = 'Y') OR
                         (NM_ADDR_TRADELINE2 = 'Y' and NM_DOB_MOBILE_DS = 'Y' ) OR
                         (NM_ADDR_TRADELINE2 = 'Y' and NM_ACCT_CHEQ = 'Y') OR
                         (NM_ADDR_TRADELINE2 = 'Y' and NM_ACCT_BNK_STMT = 'Y')) Then
                   'Dual Source - Two Trades or Dual Source 1 Trade & Other'
                WHEN
                   ((NM_DOB_TRADELINE1 = 'Y' and NM_ADDR_TU_DS = 'Y' ) OR
                         (NM_DOB_TRADELINE2 = 'Y' and NM_ADDR_TU_DS = 'Y' ) OR
                         (NM_ACCT_CHEQ = 'Y' and NM_ADDR_TU_DS = 'Y') OR
                         (NM_DOB_CRA  = 'Y' and NM_ADDR_TU_DS = 'Y') OR
                         (NM_ACCT_BNK_STMT  = 'Y' and NM_ADDR_TU_DS = 'Y')) THEN
                    'Dual Source - TU DS & Other'
                WHEN
                   ((NM_DOB_TRADELINE1 = 'Y' and NM_ADDR_MOBILE_DS = 'Y') OR
                         (NM_DOB_TRADELINE2 = 'Y' and NM_ADDR_MOBILE_DS = 'Y') OR
                         (NM_ACCT_CHEQ = 'Y' and NM_ADDR_MOBILE_DS = 'Y') or
                         (NM_DOB_CRA= 'Y' and NM_ADDR_MOBILE_DS = 'Y') OR
                         (NM_ACCT_BNK_STMT= 'Y' and NM_ADDR_MOBILE_DS = 'Y')) THEN
                    'Dual Source – MDS Name & Other'
                WHEN
                    ((NM_DOB_TRADELINE1 = 'Y' and NM_ADDR_UTILITY = 'Y') OR
                         (NM_DOB_TRADELINE2 = 'Y' and NM_ADDR_UTILITY = 'Y') OR
                         (NM_ADDR_UTILITY = 'Y' and NM_DOB_MOBILE_DS = 'Y') OR
                         (NM_ADDR_UTILITY = 'Y' and NM_ACCT_CHEQ = 'Y') OR
                         (NM_ADDR_UTILITY = 'Y' and NM_DOB_CRA = 'Y') OR
                         (NM_ADDR_UTILITY = 'Y' and NM_ACCT_CHEQ = 'Y')) THEN
                         'Dual Source - UTIL & Other'
                WHEN
                    ((NM_DOB_TRADELINE1 = 'Y' and NM_ADDR_PPTY_TAX = 'Y') OR
                         (NM_DOB_TRADELINE2 = 'Y' and NM_ADDR_PPTY_TAX = 'Y') OR
                         (NM_ADDR_PPTY_TAX = 'Y' and NM_DOB_MOBILE_DS = 'Y') OR
                         (NM_ADDR_PPTY_TAX = 'Y' and NM_ACCT_CHEQ = 'Y') OR
                         (NM_ADDR_PPTY_TAX = 'Y' and NM_DOB_CRA = 'Y') OR
                         (NM_ADDR_PPTY_TAX = 'Y' and NM_ACCT_BNK_STMT ='Y'))  THEN
                         'Dual Source - TAX & Other'
                WHEN
                   ((NM_DOB_TRADELINE1 = 'Y' and NM_ADDR_CRA = 'Y') OR
                         (NM_DOB_TRADELINE2 = 'Y' and NM_ADDR_CRA = 'Y') OR
                         (NM_ADDR_CRA = 'Y' and NM_DOB_MOBILE_DS = 'Y') OR
                         (NM_ADDR_CRA = 'Y' and NM_ACCT_CHEQ = 'Y') OR
                         (NM_ADDR_CRA = 'Y' and NM_ACCT_BNK_STMT='Y')) THEN
                         'Dual Source - CRA & Other'
                WHEN
                   ((NM_DOB_TRADELINE1 = 'Y' and NM_ACCT_BNK_STMT='Y') OR
                         (NM_DOB_TRADELINE2 = 'Y' and NM_ACCT_BNK_STMT='Y') OR
                         (NM_ACCT_BNK_STMT='Y' and NM_DOB_MOBILE_DS = 'Y')) THEN
                         'Dual Source - Bank Statement & Other'
                WHEN
                   ((NM_DOB_TRADELINE1 = 'Y' and NM_ACCT_CHEQ = 'Y') OR
                         (NM_DOB_TRADELINE2 = 'Y' and NM_ACCT_CHEQ = 'Y') OR
                         (NM_ACCT_CHEQ = 'Y' and NM_DOB_MOBILE_DS = 'Y') OR
                         (NM_DOB_CRA = 'Y' and NM_ACCT_CHEQ = 'Y')) THEN
                         'Dual Source - 1CHQ & Other'
                ELSE 'Issue'
                END as IDV_DECISION
            FROM `{TBL_STG_ADM_NFTF_DS3}`
        """
        run_bq_dml_with_log(f'4.5 Creating {TBL_STG_ADM_NFTF_DS4}',
                            f'Completed creating {TBL_STG_ADM_NFTF_DS4}', sql)

    def _4_6_create_stg_gen_val_id(self):
        sql = f""" --4.6
             CREATE OR REPLACE TABLE `{TBL_STG_GEN_VAL_ID}` as
             SELECT X.*
             FROM (
                  SELECT
                     GEN_VAL_DETAIL_V.APP_NUM,
                     GEN_VAL_DETAIL_V.ELEMENT_NAME,
                     GEN_VAL_DETAIL_V.ALPHA_VALUE,
                     GEN_VAL_DETAIL_V.GVL_DATE,
                     GEN_VAL_DETAIL_V.COMPONENT_NAME,
                     GEN_VAL_DETAIL_V.GVL_PRODUCT,
                     GEN_VAL_DETAIL_V.EXECUTION_ID,
                     row_number() OVER(PARTITION BY APP_NUM,COMPONENT_NAME,GVL_PRODUCT,ELEMENT_NAME ORDER BY EXECUTION_ID DESC) R_RANK
                     FROM `{ADMIdvConst.TBL_STG_GEN_VAL_DETAIL_NFTF}` GEN_VAL_DETAIL_V
                 WHERE
                     GEN_VAL_DETAIL_V.COMPONENT_NAME IN ('CONSUMER','DS')
                     and GEN_VAL_DETAIL_V.GVL_PRODUCT IN ('MN','HA','TUDIRCT','ENSTRM')
                     and GEN_VAL_DETAIL_V.ELEMENT_NAME IN ('D NAME AND DOB MAT 1','D NM AND ADDR MAT 1' ,
                     'D INSTITUTION NM 1_1','D ACCOUNT NUMBER 1_1',
                     'D NAME AND DOB MAT 2','D NM AND ADDR MAT 2','D INSTITUTION NM 2_1','D ACCOUNT NUMBER 2_1',
                     'DS04 SOURCE MATCH 1','DS04 SOURCE NAME 1','DS04 ACCOUNT NUM 1','DS04 DATE VERIFIED 1',
                     'DS04 SOURCE MATCH 2','DS04 SOURCE MATCH 2','DS04 SOURCE NAME 2','DS04 ACCOUNT NUM 2',
                     'DS04 DATE VERIFIED 2','RETURN DATE','DS NAME MATCH FLAG','RETURNED PHN','BRAND NAME',
                     'SUBSCRIBER ID','FIRST NAME MATCH','LAST NAME MATCH')
             ) X
             WHERE X.R_RANK = 1
                    """
        run_bq_dml_with_log(f'4.6 Creating {TBL_STG_GEN_VAL_ID}',
                            f'Completed creating {TBL_STG_GEN_VAL_ID}', sql)

    def _4_7_create_stg_adm_nftf_bu(self):
        sql = f"""  --4.7
                   CREATE OR REPLACE TABLE `{TBL_STG_ADM_NFTF_BU}` as
                    SELECT
                        BU.APA_APP_NUM,
                        BU.BU_IND
                    FROM `{ADMIdvConst.TBL_STG_ADM_APP_NFTF}` nftf
                        INNER JOIN
                                   ( SELECT  ELE_ALPHA_VALUE as BU_IND,
                                             APA_APP_NUM,
                                             EXECUTION_ID
                                    FROM `{ADMIdvConst.TBL_STG_ADM_DT_ELMT}`
                                    WHERE ELE_ELEMENT_NAME = 'BUREAU_SELECTED'
                                    ) BU  ON BU.APA_APP_NUM = nftf.APA_APP_NUM
                                        AND BU.EXECUTION_ID = nftf.EXECUTION_ID
                """
        run_bq_dml_with_log(f'4.7 Creating {TBL_STG_ADM_NFTF_BU}',
                            f'Completed creating {TBL_STG_ADM_NFTF_BU}', sql)

    def _4_8_create_stg_adm_nftf_trade(self):
        sql = f""" --4.8
                CREATE OR REPLACE TABLE `{TBL_STG_ADM_NFTF_DS_TRADE}` as
                SELECT
                   ds4.APA_APP_NUM as APP_NUM,
                   NFTF_REF.REF_NUMBER AS REF_NUMBER,
                   IFNULL(NFTF_SOURCE.NAME_ON_SOURCE,
                       CASE WHEN TEMP_ADM_NFTF_BU.BU_IND='MN' THEN 'EQ'
                            WHEN TEMP_ADM_NFTF_BU.BU_IND='HA' THEN 'TU'
                            ELSE 'N/A' END) AS NAME_ON_SOURCE,
                   NFTF_DATE.DATE_VERIFIED AS DATE_VERIFIED,
                   ds4.IDV_DECISION,
                   'Credit File - Dual Source' as IDV_METHOD,
                   1 as RANK_DS
                FROM  `{TBL_STG_ADM_NFTF_DS4}` ds4
                   INNER JOIN `{TBL_STG_ADM_NFTF_BU}` TEMP_ADM_NFTF_BU ON ds4.APA_APP_NUM=TEMP_ADM_NFTF_BU.APA_APP_NUM
                   -- ref_num
                   INNER JOIN ( SELECT
                                  APP_NUM,
                                  GVL_PRODUCT,
                                  ALPHA_VALUE AS REF_NUMBER
                              FROM `{TBL_STG_GEN_VAL_ID}` GEN_VAL_DETAIL
                              WHERE  (GEN_VAL_DETAIL.ELEMENT_NAME in ('D ACCOUNT NUMBER 1_1')
                                       AND GEN_VAL_DETAIL.COMPONENT_NAME = 'CONSUMER'
                                       AND GEN_VAL_DETAIL.GVL_PRODUCT = 'MN'
                                      -- AND R_RANK = 1
                                     )
                                    OR ( GEN_VAL_DETAIL.ELEMENT_NAME in ('DS04 ACCOUNT NUM 1')
                                        AND  GEN_VAL_DETAIL.COMPONENT_NAME = 'CONSUMER'
                                        AND GEN_VAL_DETAIL.GVL_PRODUCT = 'HA'
                                        --AND R_RANK = 1
                                    )
                              ) NFTF_REF ON ds4.APA_APP_NUM = NFTF_REF.APP_NUM
                              AND  TEMP_ADM_NFTF_BU.BU_IND = NFTF_REF.GVL_PRODUCT
                   -- verfied date
                   LEFT JOIN ( SELECT
                                  APP_NUM,
                                  GVL_DATE AS DATE_VERIFIED,
                                  GVL_PRODUCT
                              FROM `{TBL_STG_GEN_VAL_ID}` GEN_VAL_DETAIL
                              WHERE  ( GEN_VAL_DETAIL.ELEMENT_NAME in ('RETURN DATE')
                                       AND  GEN_VAL_DETAIL.COMPONENT_NAME = 'CONSUMER'
                                       AND GEN_VAL_DETAIL.GVL_PRODUCT = 'MN'
                                       --AND R_RANK = 1
                                     )
                                    OR ( GEN_VAL_DETAIL.ELEMENT_NAME in ('DS04 DATE VERIFIED 1')
                                        AND  GEN_VAL_DETAIL.COMPONENT_NAME = 'CONSUMER'
                                        AND GEN_VAL_DETAIL.GVL_PRODUCT = 'HA'
                                        --AND R_RANK = 1
                                       )
                              ) NFTF_DATE ON ds4.APA_APP_NUM = NFTF_DATE.APP_NUM
                               AND  TEMP_ADM_NFTF_BU.BU_IND = NFTF_DATE.GVL_PRODUCT
                   -- source name
                   LEFT JOIN ( SELECT
                                  APP_NUM,
                                  ALPHA_VALUE AS NAME_ON_SOURCE,
                                  GVL_PRODUCT
                              FROM `{TBL_STG_GEN_VAL_ID}` GEN_VAL_DETAIL
                              WHERE  ( GEN_VAL_DETAIL.ELEMENT_NAME in ('D INSTITUTION NM 1_1')
                                       AND  GEN_VAL_DETAIL.COMPONENT_NAME = 'CONSUMER'
                                       AND GEN_VAL_DETAIL.GVL_PRODUCT = 'MN'
                                       --AND R_RANK = 1
                                     )
                                    OR ( GEN_VAL_DETAIL.ELEMENT_NAME in ('DS04 SOURCE NAME 1')
                                        AND  GEN_VAL_DETAIL.COMPONENT_NAME = 'CONSUMER'
                                        AND GEN_VAL_DETAIL.GVL_PRODUCT = 'HA'
                                        --AND R_RANK = 1
                                       )
                              ) NFTF_SOURCE ON ds4.APA_APP_NUM = NFTF_SOURCE.APP_NUM
                               AND  TEMP_ADM_NFTF_BU.BU_IND = NFTF_SOURCE.GVL_PRODUCT
                WHERE ds4.IDV_DECISION = 'Dual Source - Two Trades or Dual Source 1 Trade & Other'
                """
        run_bq_dml_with_log(f'4.8 Creating {TBL_STG_ADM_NFTF_DS_TRADE}',
                            f'Completed creating {TBL_STG_ADM_NFTF_DS_TRADE}', sql)

    def _4_10_create_stg_adm_nftf_tuds(self):
        sql = f""" --4.10
                CREATE OR REPLACE TABLE `{TBL_STG_ADM_NFTF_DS_TUDS}` as
                SELECT
                   ds4.APA_APP_NUM as APP_NUM,
                   NFTF_REF.REF_NUMBER AS REF_NUMBER,
                   'TRANSUNION' AS NAME_ON_SOURCE,
                   NFTF_DATE.DATE_VERIFIED AS DATE_VERIFIED,
                   ds4.IDV_DECISION,
                   'Dual Source' as IDV_METHOD,
                   2 as RANK_DS
                FROM  `{TBL_STG_ADM_NFTF_DS4}` ds4
                   INNER JOIN `{TBL_STG_ADM_NFTF_BU}` TEMP_ADM_NFTF_BU ON ds4.APA_APP_NUM=TEMP_ADM_NFTF_BU.APA_APP_NUM
                   -- ref_num
                   INNER JOIN ( SELECT
                                  APP_NUM,
                                  ALPHA_VALUE AS REF_NUMBER
                              FROM `{TBL_STG_GEN_VAL_ID}` GEN_VAL_DETAIL
                              WHERE  ( GEN_VAL_DETAIL.ELEMENT_NAME in ('RETURNED PHN')
                                       AND GEN_VAL_DETAIL.COMPONENT_NAME = 'DS'
                                       AND GEN_VAL_DETAIL.GVL_PRODUCT = 'TUDIRCT'
                                       --AND R_RANK = 1
                                       )
                                  OR  ( GEN_VAL_DETAIL.ELEMENT_NAME in ('RETURNED PHN')
                                       AND GEN_VAL_DETAIL.COMPONENT_NAME = 'DS'
                                       AND GEN_VAL_DETAIL.GVL_PRODUCT = 'MANUAL'
                                       AND R_RANK = 1 )
                              ) NFTF_REF ON ds4.APA_APP_NUM = NFTF_REF.APP_NUM
                   -- verfied date
                   LEFT JOIN ( SELECT
                                  APP_NUM,
                                  GVL_DATE AS DATE_VERIFIED
                              FROM `{TBL_STG_GEN_VAL_ID}` GEN_VAL_DETAIL
                              WHERE  ( GEN_VAL_DETAIL.ELEMENT_NAME in ('RETURN DATE')
                                       AND  GEN_VAL_DETAIL.COMPONENT_NAME = 'DS'
                                       AND GEN_VAL_DETAIL.GVL_PRODUCT = 'TUDIRCT'
                                       --AND R_RANK = 1
                                       )
                                  OR  ( GEN_VAL_DETAIL.ELEMENT_NAME in ('RETURN DATE')
                                       AND  GEN_VAL_DETAIL.COMPONENT_NAME = 'MANUAL'
                                       AND GEN_VAL_DETAIL.GVL_PRODUCT = 'TUDIRCT'
                                       --AND R_RANK = 1
                                        )
                              ) NFTF_DATE ON ds4.APA_APP_NUM = NFTF_DATE.APP_NUM
                    WHERE ds4.IDV_DECISION = 'Dual Source - TU DS & Other'
                """
        run_bq_dml_with_log(f'4.10 Creating {TBL_STG_ADM_NFTF_DS_TUDS}',
                            f'Completed creating {TBL_STG_ADM_NFTF_DS_TUDS}', sql)

    def _4_12_create_stg_adm_nftf_mds(self):
        sql = f""" --4.12
                CREATE OR REPLACE TABLE `{TBL_STG_ADM_NFTF_DS_MDS}` as
                SELECT
                   ds4.APA_APP_NUM as APP_NUM,
                   NFTF_REF.REF_NUMBER AS REF_NUMBER,
                   NFTF_SOURCE.NAME_ON_SOURCE AS NAME_ON_SOURCE,
                   NFTF_DATE.DATE_VERIFIED AS DATE_VERIFIED,
                   ds4.IDV_DECISION,
                   'Dual Source' as IDV_METHOD,
                   3 as RANK_DS
                FROM  `{TBL_STG_ADM_NFTF_DS4}` ds4
                   -- ref_num
                   INNER JOIN ( SELECT
                                  APP_NUM,
                                  ALPHA_VALUE AS REF_NUMBER
                              FROM `{TBL_STG_GEN_VAL_ID}` GEN_VAL_DETAIL
                              WHERE  ( GEN_VAL_DETAIL.ELEMENT_NAME in ('SUBSCRIBER ID')
                                       AND GEN_VAL_DETAIL.COMPONENT_NAME = 'CONSUMER'
                                       AND GEN_VAL_DETAIL.GVL_PRODUCT = 'ENSTRM'
                                       --AND R_RANK = 1
                                       )
                                 OR  ( GEN_VAL_DETAIL.ELEMENT_NAME in ('SUBSCRIBER ID')
                                       AND GEN_VAL_DETAIL.COMPONENT_NAME = 'MANUAL'
                                       AND GEN_VAL_DETAIL.GVL_PRODUCT = 'ENSTRM'
                                       --AND R_RANK = 1
                                       )
                              ) NFTF_REF ON ds4.APA_APP_NUM = NFTF_REF.APP_NUM
                   -- source name
                   LEFT JOIN ( SELECT
                                  APP_NUM,
                                  ALPHA_VALUE AS NAME_ON_SOURCE
                              FROM `{TBL_STG_GEN_VAL_ID}` GEN_VAL_DETAIL
                              WHERE ( GEN_VAL_DETAIL.ELEMENT_NAME in ('BRAND NAME')
                                       AND  GEN_VAL_DETAIL.COMPONENT_NAME = 'CONSUMER'
                                       AND GEN_VAL_DETAIL.GVL_PRODUCT = 'ENSTRM'
                                       --AND R_RANK = 1
                                       )
                                 OR  ( GEN_VAL_DETAIL.ELEMENT_NAME in ('BRAND NAME')
                                       AND  GEN_VAL_DETAIL.COMPONENT_NAME = 'MANUAL'
                                       AND GEN_VAL_DETAIL.GVL_PRODUCT = 'ENSTRM'
                                       --AND R_RANK = 1
                                       )
                              ) NFTF_SOURCE ON ds4.APA_APP_NUM = NFTF_SOURCE.APP_NUM
                   -- verfied date
                   LEFT JOIN ( SELECT
                                  APP_NUM,
                                  GVL_DATE AS DATE_VERIFIED
                              FROM `{TBL_STG_GEN_VAL_ID}` GEN_VAL_DETAIL
                              WHERE  ( GEN_VAL_DETAIL.ELEMENT_NAME in ('RETURN DATE')
                                       AND  GEN_VAL_DETAIL.COMPONENT_NAME = 'CONSUMER'
                                       AND GEN_VAL_DETAIL.GVL_PRODUCT = 'ENSTRM'
                                       --AND R_RANK = 1
                                       )
                                  OR  ( GEN_VAL_DETAIL.ELEMENT_NAME in ('RETURN DATE')
                                       AND  GEN_VAL_DETAIL.COMPONENT_NAME = 'MANUAL'
                                       AND GEN_VAL_DETAIL.GVL_PRODUCT = 'ENSTRM'
                                       --AND R_RANK = 1
                                       )
                              ) NFTF_DATE ON ds4.APA_APP_NUM = NFTF_DATE.APP_NUM
                WHERE ds4.IDV_DECISION = 'Dual Source – MDS Name & Other'
                """
        run_bq_dml_with_log(f'4.12 Creating {TBL_STG_ADM_NFTF_DS_MDS}',
                            f'Completed creating {TBL_STG_ADM_NFTF_DS_MDS}', sql)

    def _4_14_pre_create_stg_adm_disp_user(self):
        sql = f"""
                   CREATE OR REPLACE TABLE {TBL_STG_ADM_STD_DISP_USER} AS
                      SELECT
                       ADM_STD_DISP_USER_V.APA_APP_NUM,
                       ADM_STD_DISP_USER_V.DTC_SEQUENCE,
                       ADM_STD_DISP_USER_V.DTC_USER_5_BYTE_7,
                       ADM_STD_DISP_USER_V.DTC_USER_5_BYTE_8,
                       ADM_STD_DISP_USER_V.DTC_USER_4_BYTE_1,
                       ADM_STD_DISP_USER_V.DTC_USER_20_BYTE_8,
                       ADM_STD_DISP_USER_V.DTC_USER_20_BYTE_9,
                       ADM_STD_DISP_USER_V.DTC_USER_DATE_5,
                       ADM_STD_DISP_USER_V.DTC_USER_DATE_6,
                       ADM_STD_DISP_USER_V.DTC_USER_DATE_7,
                       ADM_STD_DISP_USER_V.DTC_USER_DATE_8,
                       ADM_STD_DISP_USER_V.DTC_USER_20_BYTE_2,
                       ADM_STD_DISP_USER_V.DTC_USER_20_BYTE_3,
                       ADM_STD_DISP_USER_V.DTC_USER_10_BYTE_16,
                       ADM_STD_DISP_USER_V.DTC_USER_20_BYTE_1,
                       ADM_STD_DISP_USER_V.DTC_USER_20_BYTE_4,
                       ADM_STD_DISP_USER_V.DTC_USER_DATE_3,
                       EXECUTION_ID,
                       REC_CHANGE_TMS
                    FROM
                       `{ADMIdvConst.TBL_LANDING_ADM_STD_DISP_USER}` ADM_STD_DISP_USER_V
                    WHERE ADM_STD_DISP_USER_V.DTC_SEQUENCE IN (1,2,3)
                        AND EXISTS ( SELECT 1
                                     FROM `{TBL_STG_ADM_NFTF_DS4}` ds4
                                     WHERE ADM_STD_DISP_USER_V.APA_APP_NUM = ds4.APA_APP_NUM
                                         --  AND ADM_STD_DISP_USER_V.EXECUTION_ID = ds4.EXECUTION_ID
                                    )
                   """
        run_bq_dml_with_log(f'4.14 i Creating {TBL_STG_ADM_STD_DISP_USER}',
                            f'Completed creating {TBL_STG_ADM_STD_DISP_USER}', sql)

    def _4_14_create_stg_adm_nftf_ds_oth(self):
        sql = f""" --4.14
                CREATE OR REPLACE TABLE `{TBL_STG_ADM_NFTF_DS_OTH}` as
                SELECT
                    ds4.APA_APP_NUM AS APP_NUM,
                    TEMP_ADM_DISP_1.REF_NUMBER,
                    TEMP_ADM_DISP_1.DATE_VERIFIED,
                    IFNULL(TEMP_ADM_DISP_1.SOURCE_NAME,TEMP_ADM_DISP_2.TYPE_1) as NAME_ON_SOURCE,
                    ds4.IDV_DECISION,
                    'Dual Source' as IDV_METHOD,
                    4 as RANK_DS
                FROM  `{TBL_STG_ADM_NFTF_DS4}` ds4
                  INNER JOIN
                        (SELECT
                            APA_APP_NUM,
                            DTC_USER_20_BYTE_9 AS REF_NUMBER,
                            IFNULL(DTC_USER_DATE_5,DTC_USER_DATE_6) AS DATE_VERIFIED,
                            DTC_USER_20_BYTE_8 AS SOURCE_NAME,
                            row_number() OVER(PARTITION BY APA_APP_NUM ORDER BY EXECUTION_ID DESC,REC_CHANGE_TMS DESC) REC_RANK
                         FROM `{TBL_STG_ADM_STD_DISP_USER}`
                         WHERE DTC_SEQUENCE = 2
                         ) TEMP_ADM_DISP_1 ON ds4.APA_APP_NUM = TEMP_ADM_DISP_1.APA_APP_NUM
                                          --AND ds4.EXECUTION_ID = TEMP_ADM_DISP_1.EXECUTION_ID
                                          AND TEMP_ADM_DISP_1.REC_RANK = 1
                  LEFT JOIN
                        (SELECT
                            APA_APP_NUM,
                            DTC_USER_5_BYTE_7 as TYPE_1,
                            row_number() OVER(PARTITION BY APA_APP_NUM ORDER BY EXECUTION_ID DESC,REC_CHANGE_TMS DESC) REC_RANK
                         FROM `{TBL_STG_ADM_STD_DISP_USER}`
                         WHERE DTC_SEQUENCE = 2
                         ) TEMP_ADM_DISP_2 ON ds4.APA_APP_NUM = TEMP_ADM_DISP_2.APA_APP_NUM
                                          --AND ds4.EXECUTION_ID = TEMP_ADM_DISP_2.EXECUTION_ID
                                          AND TEMP_ADM_DISP_2.REC_RANK = 1
                  WHERE ds4.IDV_DECISION IN ('Dual Source - UTIL & Other','Dual Source - TAX & Other',
                                            'Dual Source - CRA & Other',
                                            'Dual Source - Bank Statement & Other','Issue')
                """
        run_bq_dml_with_log(f'4.14 Creating {TBL_STG_ADM_NFTF_DS_OTH}',
                            f'Completed creating {TBL_STG_ADM_NFTF_DS_OTH}', sql)

    def _4_16_create_stg_adm_nftf_ds_chk(self):
        sql = f""" --4.16
                CREATE OR REPLACE TABLE `{TBL_STG_ADM_NFTF_DS_CHK}` AS
                   SELECT
                       ds4.APA_APP_NUM AS APP_NUM,
                       CONCAT(ADM_STD_DISP_USER.DTC_USER_20_BYTE_2, '-'
                            ,ADM_STD_DISP_USER.DTC_USER_10_BYTE_16, '-'
                            ,ADM_STD_DISP_USER.DTC_USER_20_BYTE_3) AS REF_NUMBER,
                       ADM_STD_DISP_USER.DTC_USER_DATE_3 AS DATE_VERIFIED,
                      'CHQ' as NAME_ON_SOURCE,
                      ds4.IDV_DECISION,
                      'Dual Source' as IDV_METHOD,
                      5 as RANK_DS
                   FROM
                      `{TBL_STG_ADM_NFTF_DS4}` ds4
                      INNER JOIN ( SELECT APA_APP_NUM,
                                          DTC_USER_20_BYTE_2,
                                          DTC_USER_10_BYTE_16,
                                          DTC_USER_20_BYTE_3,
                                          DTC_USER_DATE_3,
                                    row_number() OVER(PARTITION BY APA_APP_NUM ORDER BY EXECUTION_ID DESC,REC_CHANGE_TMS DESC) REC_RANK
                                   FROM `{TBL_STG_ADM_STD_DISP_USER}`
                                   WHERE DTC_SEQUENCE = 2
                                 )  ADM_STD_DISP_USER
                           ON ds4.APA_APP_NUM = ADM_STD_DISP_USER.APA_APP_NUM
                              --AND ds4.EXECUTION_ID = ADM_STD_DISP_USER.EXECUTION_ID
                              AND ADM_STD_DISP_USER.REC_RANK = 1
                   WHERE  ds4.IDV_DECISION = 'Dual Source - 1CHQ & Other'
                  """
        run_bq_dml_with_log(f'4.16 Creating {TBL_STG_ADM_NFTF_DS_CHK}',
                            f'Completed creating {TBL_STG_ADM_NFTF_DS_CHK}', sql)

    def _4_21_1_create_stg_adm_nftf_ds_union(self):
        stg_nftf_ds_tables = [TBL_STG_ADM_NFTF_DS_TRADE, TBL_STG_ADM_NFTF_DS_TUDS, TBL_STG_ADM_NFTF_DS_MDS, TBL_STG_ADM_NFTF_DS_OTH, TBL_STG_ADM_NFTF_DS_CHK]
        sql_stg_select = """
                  SELECT APP_NUM,CAST(REF_NUMBER AS STRING) AS REF_NUMBER,
                        CAST (DATE_VERIFIED AS STRING) AS DATE_VERIFIED,
                        NAME_ON_SOURCE,IDV_DECISION,IDV_METHOD,RANK_DS
                  FROM `<stg_ds_table>`
                """
        sql_union = "UNION ALL \n".join([sql_stg_select.replace('<stg_ds_table>', x) for x in stg_nftf_ds_tables])
        sql = f"""--4.21.1
                  CREATE OR REPLACE TABLE `{TBL_STG_ADM_NFTF_DS_UNION}` AS
                  SELECT APP_NUM,
                        REF_NUMBER,
                        NAME_ON_SOURCE,
                        NAME_ON_SOURCE AS ID_TYPE,
                        NAME_ON_SOURCE AS ID_TYPE_DESCRIPTION,
                        DATE_VERIFIED,
                        NULL as ISSUE_STATE,
                        'CA' as ID_COUNTRY,
                        IDV_DECISION,
                        IDV_METHOD,
                        RANK_DS
                  FROM
                       (
                        {sql_union}
                       ) x
                """
        run_bq_dml_with_log(f'4.21.1 Creating {TBL_STG_ADM_NFTF_DS_UNION}',
                            f'Completed creating {TBL_STG_ADM_NFTF_DS_UNION}', sql)

    def _4_21_2_create_stg_adm_nftf_ds_final(self):
        sql = f""" --4.21.2
                   CREATE OR REPLACE TABLE `{TBL_STG_ADM_NFTF_DS_FINAL}` AS
                   SELECT M_DS.APP_NUM,
                          REF_NUMBER,
                          LTTR_OVRD_DETAIL_XTO.ACCOUNT_ID,
                          NAME_ON_SOURCE,
                          ID_TYPE,
                          ID_TYPE_DESCRIPTION,
                          DATE_VERIFIED,
                          ISSUE_STATE,
                          ID_COUNTRY,
                          IDV_DECISION,
                           IDV_METHOD
                   FROM
                       (SELECT APP_NUM,REF_NUMBER,DATE_VERIFIED,NAME_ON_SOURCE,IDV_DECISION,IDV_METHOD,ID_TYPE,ID_TYPE_DESCRIPTION,ISSUE_STATE,ID_COUNTRY,
                               RANK() OVER (PARTITION BY APP_NUM ORDER BY RANK_DS ASC) AS RANK_DS
                         FROM `{TBL_STG_ADM_NFTF_DS_UNION}`
                        WHERE REF_NUMBER IS NOT NULL AND REF_NUMBER != '') M_DS
                   INNER JOIN  (SELECT  APP_NUM,
                                        TS2_ACCOUNT_ID ACCOUNT_ID,
                                        row_number() OVER(PARTITION BY APP_NUM ORDER BY FILE_CREATE_DT DESC, REC_CHNG_TMS DESC) REC_RANK
                                      FROM `{ADMIdvConst.TBL_LANDING_LTTR_OVRD_DETAIL_XTO}` LTTR_OVRD_DETAIL_XTO
                                      WHERE  LTTR_OVRD_DETAIL_XTO.TS2_ACCOUNT_ID is not null
                                          AND LTTR_OVRD_DETAIL_XTO.TS2_ACCOUNT_ID <> 0
                                ) LTTR_OVRD_DETAIL_XTO
                                      ON   LTTR_OVRD_DETAIL_XTO.APP_NUM = M_DS.APP_NUM
                                          AND LTTR_OVRD_DETAIL_XTO.REC_RANK = 1
                   WHERE M_DS.RANK_DS = 1
                    """
        run_bq_dml_with_log(f'4.21.2 Creating {TBL_STG_ADM_NFTF_DS_FINAL}',
                            f'Completed creating {TBL_STG_ADM_NFTF_DS_FINAL}', sql)

    def _4_21_3_create_stg_adm_nftf_ds_excp(self):
        sql = f""" CREATE OR REPLACE TABLE `{TBL_STG_ADM_NFTF_DS_EXP}` AS
                   SELECT M_DS.APP_NUM,
                          REF_NUMBER,
                          LTTR_OVRD_DETAIL_XTO.ACCOUNT_ID
                   FROM
                       (SELECT APP_NUM,REF_NUMBER
                       FROM `{TBL_STG_ADM_NFTF_DS_UNION}`
                       ) M_DS
                   LEFT JOIN  (SELECT FILE_CREATE_DT,
                                            APP_NUM,
                                            TS2_ACCOUNT_ID ACCOUNT_ID,
                                            row_number() OVER(PARTITION BY LTTR_OVRD_DETAIL_XTO.APP_NUM ORDER BY LTTR_OVRD_DETAIL_XTO.FILE_CREATE_DT DESC, LTTR_OVRD_DETAIL_XTO.REC_CHNG_TMS DESC) REC_RANK
                                      FROM `{ADMIdvConst.TBL_LANDING_LTTR_OVRD_DETAIL_XTO}` LTTR_OVRD_DETAIL_XTO
                                      WHERE  LTTR_OVRD_DETAIL_XTO.TS2_ACCOUNT_ID is not null
                                          AND LTTR_OVRD_DETAIL_XTO.TS2_ACCOUNT_ID <> 0
                                    ) LTTR_OVRD_DETAIL_XTO
                                      ON   LTTR_OVRD_DETAIL_XTO.APP_NUM = M_DS.APP_NUM
                                          AND LTTR_OVRD_DETAIL_XTO.REC_RANK = 1
                   WHERE M_DS.REF_NUMBER IS NULL OR  LTTR_OVRD_DETAIL_XTO.ACCOUNT_ID IS NULL
                    """
        run_bq_dml_with_log(f'4.21.3 Creating {TBL_STG_ADM_NFTF_DS_EXP}',
                            f'Completed creating {TBL_STG_ADM_NFTF_DS_EXP}', sql)
