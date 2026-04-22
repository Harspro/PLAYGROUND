CREATE
OR REPLACE TABLE `{staging_table_id}` OPTIONS (
    expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
) AS (
    SELECT
        DISTINCT EFT.PCF_CUST_ID,
        EFT.ACCOUNT_NO,
        EFT.CARD_NUMBER,
        EFT.PAYER_NAME,
        EFT.AMOUNT,
        EFT.EFT_REQ_UID,
        EFT.EFT_FULFILLMENT_UID,
        EFT.EFT_REQTYPE_CD,
        EFT.FUNDSMOVE_INSTRUCTION_ID,
        EFT.FUNDSMOVE_GL_REQ_UID,
        EFT.FUNDSMOVE_PROCESSING_DT,
        EFT.INSTITUTION_ID,
        EFT.TRANSIT_NUMBER,
        EFT.EFT_ACCOUNT_NO,
        EFT.ORIGINATOR_NAME,
        EFT.ORIGINATOR_ID,
        EFT.SETTLE_ON,
        EFT.PCB_ASSIGNED_OPACCT_NICKNAME,
        EFT.OPACCT_INSTITUTION_NO,
        EFT.OPACCT_TRANSIT_NUMBER,
        EFT.OPACCT_NUMBER,
        EFT.TRACE_ID,
        EFT.EXECUTED_ON,
        EFT.REQUEST_ON,
        EFT.CREATE_DT,
        EFT.CREATE_USER_ID,
        EFT.CREATE_FUNCTION_NAME,
        EFT.UPDATE_DT,
        EXTRACT(
            MONTH
            FROM
                EFT.UPDATE_DT
        ) AS UPDATE_MONTH,
        EXTRACT(
            YEAR
            FROM
                EFT.UPDATE_DT
        ) AS UPDATE_YEAR,
        EFT.UPDATE_USER_ID,
        EFT.UPDATE_FUNCTION_NAME,
        EFT.TRANSFER_TYPE,
        EFT.CPA_TRANSACTION_TYPE,
        EFT.PCB_INSTITUTION_ID,
        EFT.PCB_TRANSIT_NUMBER,
        EFT.PCB_CUSTOMER_ACCOUNT_NUMBER,
        EFT.PCB_CUSTOMER_NAME,
        EFT.ORIGIN_CROSS_REFERENCE_NUMBER,
        EFT.ORIGIN_INSTITUTION_ID,
        EFT.ORIGIN_TRANSIT_NUMBER,
        EFT.ORIGIN_ACCOUNT_NUMBER,
        SAFE_CAST(EFT.ORIGIN_TRACE_NUMBER AS STRING) AS ORIGIN_TRACE_NUMBER,
        EFT.EFT_SOURCE,
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
        ) AS EFT
        LEFT JOIN (
            SELECT
                DISTINCT EFT_REQ_UID
            FROM
                `pcb-{env}-curated.domain_payments.BI_QUERY_EFT_RECORDS`
        ) existing_records ON EFT.EFT_REQ_UID = existing_records.EFT_REQ_UID
        LEFT JOIN (
            SELECT
                EFT_REQ_UID,
                PCF_CUST_ID,
                ACCOUNT_NO,
                CARD_NUMBER,
                FUNDSMOVE_INSTRUCTION_ID,
                FUNDSMOVE_GL_REQ_UID,
                UPDATE_DT
            FROM
                (
                    SELECT
                        EFT_REQ_UID,
                        PCF_CUST_ID,
                        ACCOUNT_NO,
                        CARD_NUMBER,
                        FUNDSMOVE_INSTRUCTION_ID,
                        FUNDSMOVE_GL_REQ_UID,
                        UPDATE_DT,
                        ROW_NUMBER() OVER (
                            PARTITION BY EFT_REQ_UID
                            ORDER BY
                                BI_QUERY_EFT_DATE DESC,
                                BI_QUERY_DAG_RUN_ID DESC
                        ) as rn
                    FROM
                        `pcb-{env}-curated.domain_payments.BI_QUERY_EFT_RECORDS`
                )
            WHERE
                rn = 1
        ) latest_records ON EFT.EFT_REQ_UID = latest_records.EFT_REQ_UID
    WHERE
        (
            -- New records (no existing record found)
            existing_records.EFT_REQ_UID IS NULL -- OR records that need updates (have new data for critical fields)
            OR (
                latest_records.EFT_REQ_UID IS NOT NULL
                AND (
                    -- Critical fields that were NULL before and now have values
                    (
                        latest_records.PCF_CUST_ID IS NULL
                        AND EFT.PCF_CUST_ID IS NOT NULL
                    )
                    OR (
                        latest_records.ACCOUNT_NO IS NULL
                        AND EFT.ACCOUNT_NO IS NOT NULL
                    )
                    OR (
                        latest_records.CARD_NUMBER IS NULL
                        AND EFT.CARD_NUMBER IS NOT NULL
                    ) -- FUNDSMOVE fields that were -1 (no match) and now have real values
                    OR (
                        latest_records.FUNDSMOVE_INSTRUCTION_ID = -1
                        AND EFT.FUNDSMOVE_INSTRUCTION_ID != -1
                    )
                    OR (
                        latest_records.FUNDSMOVE_GL_REQ_UID = -1
                        AND EFT.FUNDSMOVE_GL_REQ_UID != -1
                    ) -- General update check
                    OR latest_records.UPDATE_DT < EFT.UPDATE_DT
                )
            )
        )
);