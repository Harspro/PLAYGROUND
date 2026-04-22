CREATE OR REPLACE TABLE `pcb-{DEPLOY_ENV}-processing.domain_aml.APP_ABB` AS
        SELECT DISP_APP_DATA.APA_APP_NUM
        FROM
            (SELECT DISTINCT
               ADM_STD_DISP_APP_DATA_V.APA_APP_NUM,
               MAX(ADM_STD_DISP_APP_DATA_V.EXECUTION_ID) as MAX_EXECUTION_ID
            FROM
               `pcb-{DEPLOY_ENV}-curated.domain_customer_acquisition.ADM_STD_DISP_APP_DATA` ADM_STD_DISP_APP_DATA_V
            WHERE
               ADM_STD_DISP_APP_DATA_V.APA_STATUS = "NEWACCOUNT"
               AND ADM_STD_DISP_APP_DATA_V.APA_QUEUE_ID IN ('APPROVE', 'A2 WAIT')
               AND (ADM_STD_DISP_APP_DATA_V.APA_TEST_ACCOUNT_FLAG IS NULL OR ADM_STD_DISP_APP_DATA_V.APA_TEST_ACCOUNT_FLAG = "")
               AND ADM_STD_DISP_APP_DATA_V.FILE_CREATE_DT > DATE_SUB(DATE(CAST('{start_date}' AS DATE)), INTERVAL 7 DAY)
            GROUP BY
               ADM_STD_DISP_APP_DATA_V.APA_APP_NUM) DISP_APP_DATA
            INNER JOIN
                (SELECT DISTINCT
                   ADM_STD_DISP_DT_ELMT_V.APA_APP_NUM, EXECUTION_ID
                FROM
                   `pcb-{DEPLOY_ENV}-curated.domain_customer_acquisition.ADM_STD_DISP_DT_ELMT` ADM_STD_DISP_DT_ELMT_V
                WHERE
                   ADM_STD_DISP_DT_ELMT_V.ELE_ELEMENT_NAME IN ('ABB_IND')
                   AND ADM_STD_DISP_DT_ELMT_V.ELE_ALPHA_VALUE='Y'
                   AND ADM_STD_DISP_DT_ELMT_V.FILE_CREATE_DT > DATE_SUB(DATE('{start_date}'), INTERVAL 7 DAY)
                ) DT_ELMT
                ON DISP_APP_DATA.APA_APP_NUM = DT_ELMT.APA_APP_NUM
                   AND DISP_APP_DATA.MAX_EXECUTION_ID = DT_ELMT.EXECUTION_ID
            INNER JOIN
                (SELECT DISTINCT
                   APA_APP_NUM, EXECUTION_ID
                FROM
                   `pcb-{DEPLOY_ENV}-curated.domain_customer_acquisition.ADM_STD_DISP_BAL_TRNS_INFO` ADM_STD_DISP_BAL_TRNS_INFO
                WHERE
                   ADM_STD_DISP_BAL_TRNS_INFO.DTC_BXR_SOURCE_1='00011535'
                   AND ADM_STD_DISP_BAL_TRNS_INFO.FILE_CREATE_DT > DATE_SUB(DATE('{start_date}'), INTERVAL 7 DAY)
                ) BAL_TRNS_INFO
                ON DISP_APP_DATA.APA_APP_NUM = BAL_TRNS_INFO.APA_APP_NUM
                   AND DISP_APP_DATA.MAX_EXECUTION_ID = BAL_TRNS_INFO.EXECUTION_ID;
