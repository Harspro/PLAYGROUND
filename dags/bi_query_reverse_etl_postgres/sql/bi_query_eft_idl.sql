CREATE
OR REPLACE TABLE `{staging_table_id}` OPTIONS (
    expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
) AS (
    SELECT
        DISTINCT PCF_CUST_ID,
        ACCOUNT_NO,
        CARD_NUMBER,
        PAYER_NAME,
        AMOUNT,
        EFT_REQ_UID,
        EFT_FULFILLMENT_UID,
        EFT_REQTYPE_CD,
        FUNDSMOVE_INSTRUCTION_ID,
        FUNDSMOVE_GL_REQ_UID,
        FUNDSMOVE_PROCESSING_DT,
        INSTITUTION_ID,
        TRANSIT_NUMBER,
        EFT_ACCOUNT_NO,
        ORIGINATOR_NAME,
        ORIGINATOR_ID,
        SETTLE_ON,
        PCB_ASSIGNED_OPACCT_NICKNAME,
        OPACCT_INSTITUTION_NO,
        OPACCT_TRANSIT_NUMBER,
        OPACCT_NUMBER,
        TRACE_ID,
        EXECUTED_ON,
        REQUEST_ON,
        CREATE_DT,
        CREATE_USER_ID,
        CREATE_FUNCTION_NAME,
        UPDATE_DT,
        EXTRACT(
            MONTH
            FROM
                UPDATE_DT
        ) AS UPDATE_MONTH,
        EXTRACT(
            YEAR
            FROM
                UPDATE_DT
        ) AS UPDATE_YEAR,
        UPDATE_USER_ID,
        UPDATE_FUNCTION_NAME,
        TRANSFER_TYPE,
        CPA_TRANSACTION_TYPE,
        PCB_INSTITUTION_ID,
        PCB_TRANSIT_NUMBER,
        PCB_CUSTOMER_ACCOUNT_NUMBER,
        PCB_CUSTOMER_NAME,
        ORIGIN_CROSS_REFERENCE_NUMBER,
        ORIGIN_INSTITUTION_ID,
        ORIGIN_TRANSIT_NUMBER,
        ORIGIN_ACCOUNT_NUMBER,
        SAFE_CAST(ORIGIN_TRACE_NUMBER AS STRING) AS ORIGIN_TRACE_NUMBER,
        EFT_SOURCE,
        CURRENT_DATE('America/Toronto') AS BI_QUERY_EFT_DATE,
        '{run_id}' AS BI_QUERY_DAG_RUN_ID,
        SAFE_CAST(ABS(FARM_FINGERPRINT(GENERATE_UUID())) AS STRING) AS BI_QUERY_EFT_UID
    FROM
        (
            SELECT
                *
            FROM
                (
                    SELECT
                        DISTINCT CI.CUSTOMER_IDENTIFIER_NO AS PCF_CUST_ID,
                        A.MAST_ACCOUNT_ID AS ACCOUNT_NO,
                        FUNDSMOVE.ACCESS_MEDIUM_NO AS CARD_NUMBER,
                        EFT_IN.PAYOR_NAME AS PAYER_NAME,
                        EFT_IN.AMOUNT AS AMOUNT,
                        COALESCE(EFT_IN.EFT_PCBORIGIN_IN_REQ_UID, -1) AS EFT_REQ_UID,
                        COALESCE(EFT_IN.EFT_FULFILLMENT_UID, -1) AS EFT_FULFILLMENT_UID,
                        EFT_IN.EFT_REQTYPE_CD AS EFT_REQTYPE_CD,
                        COALESCE(FUNDSMOVE.FUNDSMOVE_INSTRUCTION_ID, -1) AS FUNDSMOVE_INSTRUCTION_ID,
                        COALESCE(FUNDSMOVE.FUNDSMOVE_GL_REQ_UID, -1) AS FUNDSMOVE_GL_REQ_UID,
                        FUNDSMOVE.FUNDSMOVE_PROCESSING_DT AS FUNDSMOVE_PROCESSING_DT,
                        EFT_IN.INSTITUTION_ID AS INSTITUTION_ID,
                        EFT_IN.TRANSIT_NUMBER AS TRANSIT_NUMBER,
                        EFT_IN.ACCOUNT_NUMBER AS EFT_ACCOUNT_NO,
                        EFT_IN.ORIGINATOR_NAME AS ORIGINATOR_NAME,
                        EFT_IN.ORIGINATOR_ID AS ORIGINATOR_ID,
                        EFT_IN.SETTLE_ON AS SETTLE_ON,
                        EFT_IN.PCB_ASSIGNED_OPACCT_NICKNAME AS PCB_ASSIGNED_OPACCT_NICKNAME,
                        EFT_IN.OPACCT_INSTITUTION_NO AS OPACCT_INSTITUTION_NO,
                        EFT_IN.OPACCT_TRANSIT_NUMBER AS OPACCT_TRANSIT_NUMBER,
                        EFT_IN.OPACCT_NUMBER AS OPACCT_NUMBER,
                        SAFE_CAST(EFT_IN.TRACE_ID AS STRING) AS TRACE_ID,
                        EFT_IN.EXECUTED_ON AS EXECUTED_ON,
                        EFT_IN.REQUEST_ON AS REQUEST_ON,
                        EFT_IN.CREATE_DT AS CREATE_DT,
                        EFT_IN.CREATE_USER_ID AS CREATE_USER_ID,
                        EFT_IN.CREATE_FUNCTION_NAME AS CREATE_FUNCTION_NAME,
                        COALESCE(EFT_IN.UPDATE_DT, '2007-01-01') AS UPDATE_DT,
                        EFT_IN.UPDATE_USER_ID AS UPDATE_USER_ID,
                        EFT_IN.UPDATE_FUNCTION_NAME AS UPDATE_FUNCTION_NAME,
                        SAFE_CAST(NULL AS STRING) AS TRANSFER_TYPE,
                        SAFE_CAST(NULL AS STRING) AS CPA_TRANSACTION_TYPE,
                        SAFE_CAST(NULL AS STRING) AS PCB_INSTITUTION_ID,
                        SAFE_CAST(NULL AS STRING) AS PCB_TRANSIT_NUMBER,
                        SAFE_CAST(NULL AS STRING) AS PCB_CUSTOMER_ACCOUNT_NUMBER,
                        SAFE_CAST(NULL AS STRING) AS PCB_CUSTOMER_NAME,
                        SAFE_CAST(NULL AS STRING) AS ORIGIN_CROSS_REFERENCE_NUMBER,
                        SAFE_CAST(NULL AS STRING) AS ORIGIN_INSTITUTION_ID,
                        SAFE_CAST(NULL AS STRING) AS ORIGIN_TRANSIT_NUMBER,
                        SAFE_CAST(NULL AS STRING) AS ORIGIN_ACCOUNT_NUMBER,
                        NULL AS ORIGIN_TRACE_NUMBER,
                        'ODS : EFT_PCBORIGIN_IN_REQ' AS EFT_SOURCE,
                        ROW_NUMBER() OVER (
                            PARTITION BY EFT_IN.EFT_PCBORIGIN_IN_REQ_UID
                            ORDER BY
                                EFT_IN.UPDATE_DT DESC
                        ) AS REC_RANK
                    FROM
                        `pcb-{env}-curated.domain_payments.EFT_PCBORIGIN_IN_REQ` EFT_IN
                        LEFT JOIN `pcb-{env}-curated.domain_payments.FUNDSMOVE_GL_REQ` FUNDSMOVE ON FUNDSMOVE.FUNDSMOVE_INSTRUCTION_ID = EFT_IN.FUNDSMOVE_INSTRUCTION_ID
                        AND FUNDSMOVE.FUNDSMOVE_PROCESSING_DT = EFT_IN.FUNDSMOVE_PROCESSING_DT
                        LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCESS_MEDIUM` AM ON AM.CARD_NUMBER = FUNDSMOVE.ACCESS_MEDIUM_NO
                        LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` AC ON AC.ACCOUNT_CUSTOMER_UID = AM.ACCOUNT_CUSTOMER_UID
                        LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCOUNT` A ON A.ACCOUNT_UID = AC.ACCOUNT_UID
                        LEFT JOIN `pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` CI ON CI.CUSTOMER_UID = AC.CUSTOMER_UID
                    WHERE
                        (
                            CI.TYPE = "PCF-CUSTOMER-ID"
                            OR CI.TYPE IS NULL
                        )
                        AND EFT_IN.EFT_REQTYPE_CD = "EPUL"
                )
            WHERE
                REC_RANK = 1
            UNION
            DISTINCT
            SELECT
                *
            FROM
                (
                    SELECT
                        DISTINCT CI.CUSTOMER_IDENTIFIER_NO AS PCF_CUST_ID,
                        A.MAST_ACCOUNT_ID AS ACCOUNT_NO,
                        EFT_FULFILLMENT.ACCESS_MEDIUM_NO AS CARD_NUMBER,
                        EFT_IN.PAYOR_NAME AS PAYER_NAME,
                        EFT_IN.AMOUNT AS AMOUNT,
                        COALESCE(EFT_IN.EFT_PCBORIGIN_IN_REQ_UID, -1) AS EFT_REQ_UID,
                        COALESCE(EFT_IN.EFT_FULFILLMENT_UID, -1) AS EFT_FULFILLMENT_UID,
                        EFT_IN.EFT_REQTYPE_CD AS EFT_REQTYPE_CD,
                        COALESCE(FUNDSMOVE.FUNDSMOVE_INSTRUCTION_ID, -1) AS FUNDSMOVE_INSTRUCTION_ID,
                        COALESCE(FUNDSMOVE.FUNDSMOVE_GL_REQ_UID, -1) AS FUNDSMOVE_GL_REQ_UID,
                        FUNDSMOVE.FUNDSMOVE_PROCESSING_DT AS FUNDSMOVE_PROCESSING_DT,
                        EFT_IN.INSTITUTION_ID AS INSTITUTION_ID,
                        EFT_IN.TRANSIT_NUMBER AS TRANSIT_NUMBER,
                        EFT_IN.ACCOUNT_NUMBER AS EFT_ACCOUNT_NO,
                        EFT_IN.ORIGINATOR_NAME AS ORIGINATOR_NAME,
                        EFT_IN.ORIGINATOR_ID AS ORIGINATOR_ID,
                        EFT_IN.SETTLE_ON AS SETTLE_ON,
                        EFT_IN.PCB_ASSIGNED_OPACCT_NICKNAME AS PCB_ASSIGNED_OPACCT_NICKNAME,
                        EFT_IN.OPACCT_INSTITUTION_NO AS OPACCT_INSTITUTION_NO,
                        EFT_IN.OPACCT_TRANSIT_NUMBER AS OPACCT_TRANSIT_NUMBER,
                        EFT_IN.OPACCT_NUMBER AS OPACCT_NUMBER,
                        SAFE_CAST(EFT_IN.TRACE_ID AS STRING) AS TRACE_ID,
                        EFT_IN.EXECUTED_ON AS EXECUTED_ON,
                        EFT_IN.REQUEST_ON AS REQUEST_ON,
                        EFT_IN.CREATE_DT AS CREATE_DT,
                        EFT_IN.CREATE_USER_ID AS CREATE_USER_ID,
                        EFT_IN.CREATE_FUNCTION_NAME AS CREATE_FUNCTION_NAME,
                        COALESCE(EFT_IN.UPDATE_DT, '2007-01-01') AS UPDATE_DT,
                        EFT_IN.UPDATE_USER_ID AS UPDATE_USER_ID,
                        EFT_IN.UPDATE_FUNCTION_NAME AS UPDATE_FUNCTION_NAME,
                        SAFE_CAST(NULL AS STRING) AS TRANSFER_TYPE,
                        SAFE_CAST(NULL AS STRING) AS CPA_TRANSACTION_TYPE,
                        SAFE_CAST(NULL AS STRING) AS PCB_INSTITUTION_ID,
                        SAFE_CAST(NULL AS STRING) AS PCB_TRANSIT_NUMBER,
                        SAFE_CAST(NULL AS STRING) AS PCB_CUSTOMER_ACCOUNT_NUMBER,
                        SAFE_CAST(NULL AS STRING) AS PCB_CUSTOMER_NAME,
                        SAFE_CAST(NULL AS STRING) AS ORIGIN_CROSS_REFERENCE_NUMBER,
                        SAFE_CAST(NULL AS STRING) AS ORIGIN_INSTITUTION_ID,
                        SAFE_CAST(NULL AS STRING) AS ORIGIN_TRANSIT_NUMBER,
                        SAFE_CAST(NULL AS STRING) AS ORIGIN_ACCOUNT_NUMBER,
                        NULL AS ORIGIN_TRACE_NUMBER,
                        'ODS : EFT_PCBORIGIN_IN_REQ' AS EFT_SOURCE,
                        ROW_NUMBER() OVER (
                            PARTITION BY EFT_IN.EFT_PCBORIGIN_IN_REQ_UID
                            ORDER BY
                                EFT_IN.UPDATE_DT DESC
                        ) AS REC_RANK
                    FROM
                        `pcb-{env}-curated.domain_payments.EFT_PCBORIGIN_IN_REQ` EFT_IN
                        LEFT JOIN `pcb-{env}-curated.domain_payments.EFT_FULFILLMENT` EFT_FULFILLMENT ON EFT_IN.EFT_FULFILLMENT_UID = EFT_FULFILLMENT.EFT_FULFILLMENT_UID
                        LEFT JOIN `pcb-{env}-curated.domain_payments.FUNDSMOVE_GL_REQ` FUNDSMOVE ON FUNDSMOVE.FUNDSMOVE_INSTRUCTION_ID = EFT_IN.FUNDSMOVE_INSTRUCTION_ID
                        AND FUNDSMOVE.FUNDSMOVE_PROCESSING_DT = EFT_IN.FUNDSMOVE_PROCESSING_DT
                        LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCESS_MEDIUM` AM ON AM.CARD_NUMBER = FUNDSMOVE.ACCESS_MEDIUM_NO
                        LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` AC ON AC.ACCOUNT_CUSTOMER_UID = AM.ACCOUNT_CUSTOMER_UID
                        LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCOUNT` A ON A.ACCOUNT_UID = AC.ACCOUNT_UID
                        LEFT JOIN `pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` CI ON CI.CUSTOMER_UID = AC.CUSTOMER_UID
                    WHERE
                        (
                            CI.TYPE = "PCF-CUSTOMER-ID"
                            OR CI.TYPE IS NULL
                        )
                        AND EFT_IN.EFT_REQTYPE_CD != 'EPUL'
                )
            WHERE
                REC_RANK = 1
            UNION
            DISTINCT
            SELECT
                *
            FROM
                (
                    SELECT
                        DISTINCT CI.CUSTOMER_IDENTIFIER_NO AS PCF_CUST_ID,
                        A.MAST_ACCOUNT_ID AS ACCOUNT_NO,
                        FUNDSMOVE.ACCESS_MEDIUM_NO AS CARD_NUMBER,
                        SAFE_CAST(NULL AS STRING) AS PAYER_NAME,
                        EFT_EXT.AMOUNT AS AMOUNT,
                        COALESCE(EFT_EXT.EFT_EXTORIGIN_REQ_UID, -1) AS EFT_REQ_UID,
                        COALESCE(EFT_EXT.EFT_FULFILLMENT_UID, -1) AS EFT_FULFILLMENT_UID,
                        EFT_EXT.EFT_REQTYPE_CD AS EFT_REQTYPE_CD,
                        COALESCE(FUNDSMOVE.FUNDSMOVE_INSTRUCTION_ID, -1) AS FUNDSMOVE_INSTRUCTION_ID,
                        COALESCE(FUNDSMOVE.FUNDSMOVE_GL_REQ_UID, -1) AS FUNDSMOVE_GL_REQ_UID,
                        FUNDSMOVE.FUNDSMOVE_PROCESSING_DT AS FUNDSMOVE_PROCESSING_DT,
                        EFT_EXT.PCB_INSTITUTION_ID AS INSTITUTION_ID,
                        EFT_EXT.PCB_TRANSIT_NUMBER AS TRANSIT_NUMBER,
                        SAFE_CAST(NULL AS STRING) AS EFT_ACCOUNT_NO,
                        EFT_EXT.ORIGIN_LONG_NAME AS ORIGINATOR_NAME,
                        EFT_EXT.ORIGINATOR_ID AS ORIGINATOR_ID,
                        SAFE.PARSE_DATETIME('%Y%j', 2 || EFT_EXT.SETTLE_ON) AS SETTLE_ON,
                        SAFE_CAST(NULL AS STRING) AS PCB_ASSIGNED_OPACCT_NICKNAME,
                        SAFE_CAST(NULL AS STRING) AS OPACCT_INSTITUTION_NO,
                        SAFE_CAST(NULL AS STRING) AS OPACCT_TRANSIT_NUMBER,
                        SAFE_CAST(NULL AS STRING) AS OPACCT_NUMBER,
                        EFT_EXT.TRACE_ID AS TRACE_ID,
                        SAFE_CAST(NULL AS DATETIME) AS EXECUTED_ON,
                        EFT_EXT.REQUEST_ON AS REQUEST_ON,
                        EFT_EXT.CREATE_DT AS CREATE_DT,
                        EFT_EXT.CREATE_USER_ID AS CREATE_USER_ID,
                        EFT_EXT.CREATE_FUNCTION_NAME AS CREATE_FUNCTION_NAME,
                        COALESCE(EFT_EXT.UPDATE_DT, '2007-01-01') AS UPDATE_DT,
                        EFT_EXT.UPDATE_USER_ID AS UPDATE_USER_ID,
                        EFT_EXT.UPDATE_FUNCTION_NAME AS UPDATE_FUNCTION_NAME,
                        EFT_EXT.TRANSFER_TYPE AS TRANSFER_TYPE,
                        EFT_EXT.CPA_TRANSACTION_TYPE AS CPA_TRANSACTION_TYPE,
                        EFT_EXT.PCB_INSTITUTION_ID AS PCB_INSTITUTION_ID,
                        EFT_EXT.PCB_TRANSIT_NUMBER AS PCB_TRANSIT_NUMBER,
                        EFT_EXT.PCB_CUSTOMER_ACCOUNT_NUMBER AS PCB_CUSTOMER_ACCOUNT_NUMBER,
                        EFT_EXT.PCB_CUSTOMER_NAME AS PCB_CUSTOMER_NAME,
                        EFT_EXT.ORIGIN_CROSS_REFERENCE_NUMBER AS ORIGIN_CROSS_REFERENCE_NUMBER,
                        EFT_EXT.ORIGIN_INSTITUTION_ID AS ORIGIN_INSTITUTION_ID,
                        EFT_EXT.ORIGIN_TRANSIT_NUMBER AS ORIGIN_TRANSIT_NUMBER,
                        EFT_EXT.ORIGIN_ACCOUNT_NUMBER AS ORIGIN_ACCOUNT_NUMBER,
                        EFT_EXT.ORIGIN_TRACE_NUMBER AS ORIGIN_TRACE_NUMBER,
                        'ODS : EFT_EXTORIGIN_REQ' AS EFT_SOURCE,
                        ROW_NUMBER() OVER (
                            PARTITION BY EFT_EXT.EFT_EXTORIGIN_REQ_UID
                            ORDER BY
                                EFT_EXT.UPDATE_DT DESC
                        ) AS REC_RANK
                    FROM
                        `pcb-{env}-curated.domain_payments.EFT_EXTORIGIN_REQ` EFT_EXT
                        LEFT JOIN `pcb-{env}-curated.domain_payments.FUNDSMOVE_GL_REQ` FUNDSMOVE ON FUNDSMOVE.FUNDSMOVE_INSTRUCTION_ID = EFT_EXT.FUNDSMOVE_INSTRUCTION_ID
                        LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCESS_MEDIUM` AM ON AM.CARD_NUMBER = FUNDSMOVE.ACCESS_MEDIUM_NO
                        LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` AC ON AC.ACCOUNT_CUSTOMER_UID = AM.ACCOUNT_CUSTOMER_UID
                        LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCOUNT` A ON A.ACCOUNT_UID = AC.ACCOUNT_UID
                        LEFT JOIN `pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` CI ON CI.CUSTOMER_UID = AC.CUSTOMER_UID
                    WHERE
                        CI.TYPE = "PCF-CUSTOMER-ID"
                        OR CI.TYPE IS NULL
                )
            WHERE
                REC_RANK = 1
        )
);