CREATE
OR REPLACE TABLE `{staging_table_id}` OPTIONS (
    expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
) AS (
    SELECT
        DISTINCT PAPER_PAYMENT.PCF_CUST_ID,
        PAPER_PAYMENT.ACCOUNT_NO,
        PAPER_PAYMENT.CARD_NUMBER,
        PAPER_PAYMENT.AMOUNT,
        PAPER_PAYMENT.SEQ_NUM,
        PAPER_PAYMENT.CHEQUE_NUM,
        PAPER_PAYMENT.CHEQUE_DATE,
        PAPER_PAYMENT.PROCESS_DATE,
        PAPER_PAYMENT.TRANSIT_NUMBER,
        PAPER_PAYMENT.INSTITUTION_ID,
        PAPER_PAYMENT.FDR_FLAG,
        PAPER_PAYMENT.FDR_DATE,
        PAPER_PAYMENT.CREATE_DT,
        PAPER_PAYMENT.UPDATE_USER_ID,
        PAPER_PAYMENT.UPDATE_DT,
        EXTRACT(
            MONTH
            FROM
                PAPER_PAYMENT.UPDATE_DT
        ) AS UPDATE_MONTH,
        EXTRACT(
            YEAR
            FROM
                PAPER_PAYMENT.UPDATE_DT
        ) AS UPDATE_YEAR,
        PAPER_PAYMENT.ST_CD,
        PAPER_PAYMENT.ST_DT,
        PAPER_PAYMENT.CSH_IND,
        SAFE_CAST(PAPER_PAYMENT.CREATE_USER_ID AS STRING) AS CREATE_USER_ID,
        SAFE_CAST(PAPER_PAYMENT.CREATE_FUNCTION_NAME AS STRING) AS CREATE_FUNCTION_NAME,
        SAFE_CAST(PAPER_PAYMENT.TRACE_ID AS STRING) AS TRACE_ID,
        SAFE_CAST(PAPER_PAYMENT.UPDATE_FUNCTION_NAME AS STRING) AS UPDATE_FUNCTION_NAME,
        PAPER_PAYMENT.PAPER_PAYMENT_SOURCE,
        CURRENT_DATE('America/Toronto') AS BI_QUERY_PAPER_PAYMENT_DATE,
        '{run_id}' AS BI_QUERY_DAG_RUN_ID,
        SAFE_CAST(ABS(FARM_FINGERPRINT(GENERATE_UUID())) AS STRING) AS BI_QUERY_PAPER_PAYMENT_UID
    FROM
        (
            SELECT
                DISTINCT CI.CUSTOMER_IDENTIFIER_NO AS PCF_CUST_ID,
                A.MAST_ACCOUNT_ID AS ACCOUNT_NO,
                COALESCE(PAPER.ACCESS_MEDIUM_NO, '-1') AS CARD_NUMBER,
                PAPER.AMOUNT AS AMOUNT,
                COALESCE(PAPER.SEQ_NUM, '-1') AS SEQ_NUM,
                PAPER.CHEQUE_NUM AS CHEQUE_NUM,
                PAPER.CHEQUE_DATE AS CHEQUE_DATE,
                DATETIME(PAPER.PROCESS_DATE) AS PROCESS_DATE,
                PAPER.TRANSIT_NUMBER AS TRANSIT_NUMBER,
                PAPER.INSTITUTION_ID AS INSTITUTION_ID,
                PAPER.FDR_FLAG AS FDR_FLAG,
                PAPER.FDR_DATE AS FDR_DATE,
                PAPER.REC_CREATE_TMS AS CREATE_DT,
                PAPER.REC_CHNG_ID AS UPDATE_USER_ID,
                COALESCE(PAPER.REC_CHNG_TMS, '2007-01-01') AS UPDATE_DT,
                PAPER.ST_CD AS ST_CD,
                PAPER.ST_DT AS ST_DT,
                PAPER.CSH_IND AS CSH_IND,
                NULL AS CREATE_USER_ID,
                NULL AS CREATE_FUNCTION_NAME,
                NULL AS TRACE_ID,
                NULL AS UPDATE_FUNCTION_NAME,
                '053 : PAPER_PYMT_TX' AS PAPER_PAYMENT_SOURCE,
                ROW_NUMBER() OVER (
                    PARTITION BY PAPER.ACCESS_MEDIUM_NO,
                    PAPER.SEQ_NUM
                    ORDER BY
                        PAPER.REC_CHNG_TMS DESC
                ) AS REC_RANK
            FROM
                `pcb-{env}-curated.domain_payments.PAPER_PYMT_TX` PAPER
                LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCESS_MEDIUM` AM ON AM.CARD_NUMBER = PAPER.ACCESS_MEDIUM_NO
                LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` AC ON AC.ACCOUNT_CUSTOMER_UID = AM.ACCOUNT_CUSTOMER_UID
                LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCOUNT` A ON A.ACCOUNT_UID = AC.ACCOUNT_UID
                LEFT JOIN `pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` CI ON CI.CUSTOMER_UID = AC.CUSTOMER_UID
            WHERE
                CI.TYPE = "PCF-CUSTOMER-ID"
                OR CI.TYPE IS NULL
        ) AS PAPER_PAYMENT
        LEFT JOIN (
            SELECT
                DISTINCT CARD_NUMBER,
                SEQ_NUM
            FROM
                `pcb-{env}-curated.domain_payments.BI_QUERY_PAPER_PAYMENT_RECORDS`
        ) existing_records ON PAPER_PAYMENT.CARD_NUMBER = existing_records.CARD_NUMBER
        AND PAPER_PAYMENT.SEQ_NUM = existing_records.SEQ_NUM
        LEFT JOIN (
            SELECT
                CARD_NUMBER,
                SEQ_NUM,
                PCF_CUST_ID,
                ACCOUNT_NO,
                UPDATE_DT
            FROM
                (
                    SELECT
                        CARD_NUMBER,
                        SEQ_NUM,
                        PCF_CUST_ID,
                        ACCOUNT_NO,
                        UPDATE_DT,
                        ROW_NUMBER() OVER (
                            PARTITION BY CARD_NUMBER,
                            SEQ_NUM
                            ORDER BY
                                BI_QUERY_PAPER_PAYMENT_DATE DESC,
                                BI_QUERY_DAG_RUN_ID DESC
                        ) as rn
                    FROM
                        `pcb-{env}-curated.domain_payments.BI_QUERY_PAPER_PAYMENT_RECORDS`
                )
            WHERE
                rn = 1
        ) latest_records ON PAPER_PAYMENT.CARD_NUMBER = latest_records.CARD_NUMBER
        AND PAPER_PAYMENT.SEQ_NUM = latest_records.SEQ_NUM
    WHERE
        PAPER_PAYMENT.REC_RANK = 1
        AND (
            -- New records (no existing record found)
            existing_records.CARD_NUMBER IS NULL -- OR records that need updates (have new data for critical fields)
            OR (
                latest_records.CARD_NUMBER IS NOT NULL
                AND (
                    -- Critical fields that were NULL before and now have values
                    (
                        latest_records.PCF_CUST_ID IS NULL
                        AND PAPER_PAYMENT.PCF_CUST_ID IS NOT NULL
                    )
                    OR (
                        latest_records.ACCOUNT_NO IS NULL
                        AND PAPER_PAYMENT.ACCOUNT_NO IS NOT NULL
                    )
                    OR (
                        latest_records.CARD_NUMBER IS NULL
                        AND PAPER_PAYMENT.CARD_NUMBER IS NOT NULL
                    ) -- General update check
                    OR latest_records.UPDATE_DT < PAPER_PAYMENT.UPDATE_DT
                )
            )
        )
);