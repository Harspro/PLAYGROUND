BEGIN
  DECLARE report_month DATE;

DECLARE CODE_PREFIX STRING;
DECLARE TRNS_ACCT_FUNC_COND ARRAY<STRING>;
DECLARE BALANCE_CODE_0 INT64;
DECLARE BALANCE_CODE_1 INT64;
DECLARE BALANCE_CODE_2 INT64;
DECLARE BALANCE_CODE_3 INT64;
DECLARE BALANCE_CODE_4 INT64;

SET CODE_PREFIX = CASE WHEN '{mode}' = 'PCMA' THEN '100' ELSE '' END;
SET TRNS_ACCT_FUNC_COND = [
  'PFC', 'CFC', 'FCA', 'FDA', 'FCV', 'FDV', 'CCF', 'CPF', 'WCF', 'WPF'];

SET BALANCE_CODE_0 = CAST(CONCAT(CODE_PREFIX, '0') AS INT64);
SET BALANCE_CODE_1 = CAST(CONCAT(CODE_PREFIX, '1') AS INT64);
SET BALANCE_CODE_2 = CAST(CONCAT(CODE_PREFIX, '2') AS INT64);
SET BALANCE_CODE_3 = CAST(CONCAT(CODE_PREFIX, '3') AS INT64);
SET BALANCE_CODE_4 = CAST(CONCAT(CODE_PREFIX, '4') AS INT64);

SET report_month = (
  SELECT (get_report_month).report_month
  FROM
    (
      SELECT
        `pcb-{env}-landing.domain_scoring.get_report_month`(
          '{report_year}', '{report_month}')
          AS get_report_month
    )
);

INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_TSYS_TRANS`
(
  MAST_ACCOUNT_ID,
  PROCESS_DT,
  INTERCHG,
  FOREX,
  MTHEND_CO_REC,
  MTHEND_BK_REC,
  MTHEND_FR_REC,
  MTHEND_INT,
  MTHEND_PYMT,
  MTHEND_MRCH,
  MTHEND_CASH,
  MTHEND_TRANS,
  CIBC_CASH_CNT,
  FRGN_CASH_CNT,
  CDN_CASH_CNT,
  OLIM_FEE,
  ABP_FEE,
  CASH_FEE,
  NSF_FEE,
  CRS_FEE,
  IDREST_FEE,
  CREDALRT_FEE,
  RSASST_FEE,
  OTHR_FEE,
  MTHEND_CONVC,
  MTHEND_BTFER,
  ANNUAL_FEE,
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
-- TRANSACTION TABLE
WITH
  WK_TSYS_TRANS3 AS (
    SELECT DISTINCT
      DATE_TRUNC(at31.FILE_CREATE_DT, MONTH) AS PROCESS_DT,
      DATE(at31.AT31_DATE_POST) AS AT31_DATE_POST,
      CAST(at31.MAST_ACCOUNT_ID AS INTEGER) AS MAST_ACCOUNT_ID,
      at31.AT00_APPLICATION_SUFFIX,
      at31.AT31_DEBIT_CREDIT_INDICATOR,
      at31.AT31_TERMS_BALANCE_CODE,
      at31.AT31_TRANSACTION_CATEGORY,
      at31.AT31_TRANSACTION_CODE,
      at31.AT31_ENHANCEMENT_ID,
      at31.AT31_TRNS_ACCT_FUNC,
      CASE
        WHEN at31.AT31_CURRENCY_CODE = 124 THEN 1
        ELSE 0
        END
        AS CDN_IND,
      CASE
        WHEN
          at31.AT31_TERMS_BALANCE_CODE = BALANCE_CODE_2
          AND at31.AT31_TRANSACTION_CODE = 101
          THEN 1
        ELSE 0
        END
        AS WRONG_IND,
      -- CONVERT(AT31_MERCHANT_ID,'US7ASCII','WE8ISO8859P1') AS AT31_MERCHANT_ID ,
      at31.AT31_MERCHANT_ID,
      SUM(
        (at31.AT31_INTERCHG_PERCENTAGE_RATE / 100) * at31.AT31_AMT_TRANSACTION)
        AS INTERCHG,
      SUM(at31.AT31_TRAN_MARK_UP_AMT) AS FOREX,
      SUM(at31.AT31_AMT_TRANSACTION) AS TRANS_AMT,
      COUNT(*) AS TRANS_CNT
    FROM
      `pcb-{env}-curated.domain_account_management.AT31` AS at31
    WHERE
      DATE_TRUNC(at31.FILE_CREATE_DT, MONTH)
      = DATE_TRUNC(DATE(report_month), MONTH)
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
  ),
  WK_TSYS_TRANS2 AS (
    SELECT DISTINCT
      wk_tsys_trans3.* EXCEPT (FOREX, INTERCHG),
      wk_tsys_chgoff_full_latest.AM00_APPLICATION_SUFFIX,
      wk_tsys_chgoff_full_latest.AM00_STATC_CHARGEOFF,
      DATE(wk_tsys_chgoff_full_latest.CHGOFF_DT) AS CHGOFF_DT,
      CASE
        WHEN wk_tsys_trans3.AT31_DEBIT_CREDIT_INDICATOR = 'C' THEN -FOREX
        ELSE FOREX
        END
        AS FOREX,
      CASE
        WHEN wk_tsys_trans3.AT31_DEBIT_CREDIT_INDICATOR = 'C' THEN -INTERCHG
        ELSE INTERCHG
        END
        AS INTERCHG,
      CASE
        WHEN wk_tsys_trans3.AT31_DEBIT_CREDIT_INDICATOR = 'C' THEN -TRANS_AMT
        ELSE TRANS_AMT
        END
        AS AMT,
      CASE
        WHEN wk_tsys_trans3.AT31_DEBIT_CREDIT_INDICATOR = 'C' THEN TRANS_AMT
        ELSE -TRANS_AMT
        END
        AS AMT2,
      wk_tsys_trans3.TRANS_CNT AS CNT,
    FROM
      WK_TSYS_TRANS3 AS wk_tsys_trans3
    LEFT JOIN
      `pcb-{env}-curated.domain_scoring.SCORING_PREP_WK_TSYS_CHGOFF_FULL_LATEST`
        AS wk_tsys_chgoff_full_latest
      ON
        CAST(wk_tsys_trans3.MAST_ACCOUNT_ID AS STRING)
          = wk_tsys_chgoff_full_latest.MAST_ACCOUNT_ID
        AND CAST(wk_tsys_trans3.AT00_APPLICATION_SUFFIX AS STRING)
          = wk_tsys_chgoff_full_latest.AM00_APPLICATION_SUFFIX
        AND wk_tsys_trans3.PROCESS_DT
          = DATE(wk_tsys_chgoff_full_latest.PROCESS_DT)
    WHERE
      wk_tsys_trans3.WRONG_IND != 1
  ),
  WK_TSYS_TRANS_BAD_ACCOUNT AS (
    SELECT DISTINCT
      wk_tsys_trans2.MAST_ACCOUNT_ID,
      wk_tsys_trans2.PROCESS_DT,
      NULL AS INTERCHG,
      NULL AS FOREX,
      CASE
        WHEN wk_tsys_trans2.AM00_STATC_CHARGEOFF IN ('DC', 'BD', 'ST') THEN AMT2
        END
        AS MTHEND_CO_REC,
      CASE
        WHEN wk_tsys_trans2.AM00_STATC_CHARGEOFF IN ('07', '13') THEN AMT2
        END
        AS MTHEND_BK_REC,
      CASE
        WHEN wk_tsys_trans2.AM00_STATC_CHARGEOFF IN ('FR') THEN AMT2
        END
        AS MTHEND_FR_REC,
      NULL AS MTHEND_INT,
      NULL AS MTHEND_PYMT,
      NULL AS MTHEND_MRCH,
      NULL AS MTHEND_CASH,
      NULL AS MTHEND_CONVC,
      NULL AS MTHEND_BTFER,
      NULL AS CIBC_CASH_CNT,
      NULL AS FRGN_CASH_CNT,
      NULL AS CDN_CASH_CNT,
      NULL AS MTHEND_TRANS,
      NULL AS OLIM_FEE,
      NULL AS ABP_FEE,
      NULL AS CASH_FEE,
      NULL AS NSF_FEE,
      NULL AS CRS_FEE,
      NULL AS IDREST_FEE,
      NULL AS CREDALRT_FEE,
      NULL AS RSASST_FEE,
      NULL AS OTHR_FEE,
      NULL AS ANNUAL_FEE
    FROM
      WK_TSYS_TRANS2 AS wk_tsys_trans2
    WHERE
      NULLIF(TRIM(wk_tsys_trans2.AM00_STATC_CHARGEOFF), "") IS NOT NULL
      AND wk_tsys_trans2.CHGOFF_DT
        <= wk_tsys_trans2.AT31_DATE_POST
  ),
  WK_TSYS_TRANS_GOOD_ACCOUNT AS (
    SELECT DISTINCT
      wk_tsys_trans2.MAST_ACCOUNT_ID,
      wk_tsys_trans2.PROCESS_DT,
      wk_tsys_trans2.INTERCHG,
      wk_tsys_trans2.FOREX,
      NULL AS MTHEND_CO_REC,
      NULL AS MTHEND_BK_REC,
      NULL AS MTHEND_FR_REC,
      CASE
        WHEN wk_tsys_trans2.AT31_TRNS_ACCT_FUNC IN UNNEST(TRNS_ACCT_FUNC_COND)
          THEN wk_tsys_trans2.AMT
        END
        AS MTHEND_INT,
      CASE
        WHEN
          wk_tsys_trans2.AT31_TRNS_ACCT_FUNC NOT IN UNNEST(TRNS_ACCT_FUNC_COND)
          AND wk_tsys_trans2.AT31_TERMS_BALANCE_CODE
            = BALANCE_CODE_0
          THEN wk_tsys_trans2.AMT2
        END
        AS MTHEND_PYMT,
      CASE
        WHEN
          wk_tsys_trans2.AT31_TRNS_ACCT_FUNC NOT IN UNNEST(TRNS_ACCT_FUNC_COND)
          AND wk_tsys_trans2.AT31_TERMS_BALANCE_CODE
            = BALANCE_CODE_1
          THEN wk_tsys_trans2.AMT
        END
        AS MTHEND_MRCH,
      CASE
        WHEN
          (
            wk_tsys_trans2.AT31_TRNS_ACCT_FUNC
                NOT IN UNNEST(TRNS_ACCT_FUNC_COND)
              AND wk_tsys_trans2.AT31_TERMS_BALANCE_CODE
                = BALANCE_CODE_2
            OR wk_tsys_trans2.AT31_TERMS_BALANCE_CODE
              >= BALANCE_CODE_4)
          THEN wk_tsys_trans2.AMT
        END
        AS MTHEND_CASH,
      CASE
        WHEN
          (
            wk_tsys_trans2.AT31_TRNS_ACCT_FUNC
              NOT IN UNNEST(TRNS_ACCT_FUNC_COND)
            AND wk_tsys_trans2.AT31_TERMS_BALANCE_CODE
              = BALANCE_CODE_2
            AND wk_tsys_trans2.AT31_TRANSACTION_CATEGORY = 4)
          OR (
            wk_tsys_trans2.AT31_TERMS_BALANCE_CODE
              >= BALANCE_CODE_4
            AND wk_tsys_trans2.AT31_TRANSACTION_CATEGORY < 6000)
          THEN wk_tsys_trans2.AMT
        END
        AS MTHEND_CONVC,
      CASE
        WHEN
          (
            wk_tsys_trans2.AT31_TRNS_ACCT_FUNC
              NOT IN UNNEST(TRNS_ACCT_FUNC_COND)
            AND wk_tsys_trans2.AT31_TERMS_BALANCE_CODE
              = BALANCE_CODE_2
            AND wk_tsys_trans2.AT31_TRANSACTION_CATEGORY = 5)
          OR (
            wk_tsys_trans2.AT31_TERMS_BALANCE_CODE
              >= BALANCE_CODE_4
            AND wk_tsys_trans2.AT31_TRANSACTION_CATEGORY >= 6000)
          THEN wk_tsys_trans2.AMT
        END
        AS MTHEND_BTFER,
      CASE
        WHEN
          wk_tsys_trans2.AT31_TRNS_ACCT_FUNC NOT IN UNNEST(TRNS_ACCT_FUNC_COND)
          AND wk_tsys_trans2.AT31_TERMS_BALANCE_CODE
            = BALANCE_CODE_2
          AND wk_tsys_trans2.AT31_TRANSACTION_CATEGORY = 6702
          THEN wk_tsys_trans2.CNT
        END
        AS CIBC_CASH_CNT,
      CASE
        WHEN
          wk_tsys_trans2.AT31_TRNS_ACCT_FUNC NOT IN UNNEST(TRNS_ACCT_FUNC_COND)
          AND wk_tsys_trans2.AT31_TERMS_BALANCE_CODE
            = BALANCE_CODE_2
          AND NOT wk_tsys_trans2.AT31_TRANSACTION_CATEGORY = 6702
          AND wk_tsys_trans2.CDN_IND = 0
          THEN wk_tsys_trans2.CNT
        END
        AS FRGN_CASH_CNT,
      CASE
        WHEN
          wk_tsys_trans2.AT31_TRNS_ACCT_FUNC NOT IN UNNEST(TRNS_ACCT_FUNC_COND)
          AND wk_tsys_trans2.AT31_TERMS_BALANCE_CODE
            = BALANCE_CODE_2
          AND NOT wk_tsys_trans2.AT31_TRANSACTION_CATEGORY = 6702
          AND wk_tsys_trans2.CDN_IND = 1
          THEN wk_tsys_trans2.CNT
        END
        AS CDN_CASH_CNT,
      CASE
        WHEN
          wk_tsys_trans2.AT31_TERMS_BALANCE_CODE
          = BALANCE_CODE_1
          THEN wk_tsys_trans2.CNT
        END
        AS MTHEND_TRANS,
      CASE
        WHEN
          wk_tsys_trans2.AT31_TERMS_BALANCE_CODE
            = BALANCE_CODE_3
          AND wk_tsys_trans2.AT31_TRANSACTION_CATEGORY = 8
          THEN wk_tsys_trans2.AMT
        END
        AS OLIM_FEE,
      CASE
        WHEN
          wk_tsys_trans2.AT31_TERMS_BALANCE_CODE
            = BALANCE_CODE_3
          AND wk_tsys_trans2.AT31_TRANSACTION_CATEGORY = 9
          THEN wk_tsys_trans2.AMT
        END
        AS ABP_FEE,
      CASE
        WHEN
          wk_tsys_trans2.AT31_TERMS_BALANCE_CODE
            = BALANCE_CODE_3
          AND wk_tsys_trans2.AT31_TRANSACTION_CATEGORY = 10
          THEN wk_tsys_trans2.AMT
        END
        AS CASH_FEE,
      CASE
        WHEN
          wk_tsys_trans2.AT31_TERMS_BALANCE_CODE
            = BALANCE_CODE_3
          AND wk_tsys_trans2.AT31_TRANSACTION_CATEGORY = 11
          THEN wk_tsys_trans2.AMT
        END
        AS NSF_FEE,
      CASE
        WHEN
          wk_tsys_trans2.AT31_TERMS_BALANCE_CODE
            = BALANCE_CODE_3
          AND wk_tsys_trans2.AT31_TRANSACTION_CATEGORY = 12
          THEN wk_tsys_trans2.AMT
        END
        AS ANNUAL_FEE,
      CASE
        WHEN
          wk_tsys_trans2.AT31_TERMS_BALANCE_CODE
            = BALANCE_CODE_3
          AND wk_tsys_trans2.AT31_TRANSACTION_CATEGORY = 48
          AND wk_tsys_trans2.AT31_ENHANCEMENT_ID = '0001'
          THEN wk_tsys_trans2.AMT
        END
        AS CRS_FEE,
      CASE
        WHEN
          wk_tsys_trans2.AT31_TERMS_BALANCE_CODE
            = BALANCE_CODE_3
          AND wk_tsys_trans2.AT31_TRANSACTION_CATEGORY = 48
          AND wk_tsys_trans2.AT31_ENHANCEMENT_ID = '0002'
          THEN wk_tsys_trans2.AMT
        END
        AS IDREST_FEE,
      CASE
        WHEN
          wk_tsys_trans2.AT31_TERMS_BALANCE_CODE
            = BALANCE_CODE_3
          AND wk_tsys_trans2.AT31_TRANSACTION_CATEGORY = 48
          AND wk_tsys_trans2.AT31_ENHANCEMENT_ID = '0003'
          THEN wk_tsys_trans2.AMT
        END
        AS CREDALRT_FEE,
      CASE
        WHEN
          wk_tsys_trans2.AT31_TERMS_BALANCE_CODE
            = BALANCE_CODE_3
          AND wk_tsys_trans2.AT31_TRANSACTION_CATEGORY = 48
          AND wk_tsys_trans2.AT31_ENHANCEMENT_ID = '0004'
          THEN wk_tsys_trans2.AMT
        END
        AS RSASST_FEE,
      CASE
        WHEN
          wk_tsys_trans2.AT31_TERMS_BALANCE_CODE
            = BALANCE_CODE_3
          AND (
            wk_tsys_trans2.AT31_TRANSACTION_CATEGORY
              NOT IN (8, 9, 10, 11, 12, 48)
            OR (
              wk_tsys_trans2.AT31_TRANSACTION_CATEGORY = 48
              AND wk_tsys_trans2.AT31_ENHANCEMENT_ID
                NOT IN ('0001', '0002', '0003', '0004')))
          THEN wk_tsys_trans2.AMT
        END
        AS OTHR_FEE,
    FROM
      WK_TSYS_TRANS2 AS wk_tsys_trans2
    WHERE
      NOT (
        NULLIF(TRIM(wk_tsys_trans2.AM00_STATC_CHARGEOFF), "") IS NOT NULL
        AND wk_tsys_trans2.CHGOFF_DT
          <= wk_tsys_trans2.AT31_DATE_POST)
  ),
  WK_TSYS_TRANS1 AS (
    SELECT DISTINCT
      *
    FROM
      WK_TSYS_TRANS_GOOD_ACCOUNT
    UNION ALL
    SELECT DISTINCT
      *
    FROM
      WK_TSYS_TRANS_BAD_ACCOUNT
  )
SELECT DISTINCT
  CAST(wk_tsys_trans1.MAST_ACCOUNT_ID AS STRING) AS MAST_ACCOUNT_ID,
  CAST(wk_tsys_trans1.PROCESS_DT AS STRING) AS PROCESS_DT,
  CAST(SUM(wk_tsys_trans1.INTERCHG) AS STRING) AS INTERCHG,
  CAST(SUM(wk_tsys_trans1.FOREX) AS STRING) AS FOREX,
  CAST(SUM(wk_tsys_trans1.MTHEND_CO_REC) AS STRING) AS MTHEND_CO_REC,
  CAST(SUM(wk_tsys_trans1.MTHEND_BK_REC) AS STRING) AS MTHEND_BK_REC,
  CAST(SUM(wk_tsys_trans1.MTHEND_FR_REC) AS STRING) AS MTHEND_FR_REC,
  CAST(SUM(wk_tsys_trans1.MTHEND_INT) AS STRING) AS MTHEND_INT,
  CAST(SUM(wk_tsys_trans1.MTHEND_PYMT) AS STRING) AS MTHEND_PYMT,
  CAST(SUM(wk_tsys_trans1.MTHEND_MRCH) AS STRING) AS MTHEND_MRCH,
  CAST(SUM(wk_tsys_trans1.MTHEND_CASH) AS STRING) AS MTHEND_CASH,
  CAST(
    SUM(wk_tsys_trans1.MTHEND_TRANS)
    + SUM(wk_tsys_trans1.FRGN_CASH_CNT)
    + SUM(wk_tsys_trans1.CDN_CASH_CNT)
    AS STRING) AS MTHEND_TRANS,
  CAST(SUM(wk_tsys_trans1.CIBC_CASH_CNT) AS STRING) AS CIBC_CASH_CNT,
  CAST(SUM(wk_tsys_trans1.FRGN_CASH_CNT) AS STRING) AS FRGN_CASH_CNT,
  CAST(SUM(wk_tsys_trans1.CDN_CASH_CNT) AS STRING) AS CDN_CASH_CNT,
  CAST(SUM(wk_tsys_trans1.OLIM_FEE) AS STRING) AS OLIM_FEE,
  CAST(SUM(wk_tsys_trans1.ABP_FEE) AS STRING) AS ABP_FEE,
  CAST(SUM(wk_tsys_trans1.CASH_FEE) AS STRING) AS CASH_FEE,
  CAST(SUM(wk_tsys_trans1.NSF_FEE) AS STRING) AS NSF_FEE,
  CAST(SUM(wk_tsys_trans1.CRS_FEE) AS STRING) AS CRS_FEE,
  CAST(SUM(wk_tsys_trans1.IDREST_FEE) AS STRING) AS IDREST_FEE,
  CAST(SUM(wk_tsys_trans1.CREDALRT_FEE) AS STRING) AS CREDALRT_FEE,
  CAST(SUM(wk_tsys_trans1.RSASST_FEE) AS STRING) AS RSASST_FEE,
  CAST(SUM(wk_tsys_trans1.OTHR_FEE) AS STRING) AS OTHR_FEE,
  CAST(SUM(wk_tsys_trans1.MTHEND_CONVC) AS STRING) AS MTHEND_CONVC,
  CAST(SUM(wk_tsys_trans1.MTHEND_BTFER) AS STRING) AS MTHEND_BTFER,
  CAST(SUM(wk_tsys_trans1.ANNUAL_FEE) AS STRING) AS ANNUAL_FEE,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  WK_TSYS_TRANS1 AS wk_tsys_trans1
GROUP BY
  wk_tsys_trans1.MAST_ACCOUNT_ID,
  wk_tsys_trans1.PROCESS_DT;

END