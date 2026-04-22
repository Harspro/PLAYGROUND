DECLARE
  source_tables ARRAY<STRING>;

DECLARE
  idx INT64 DEFAULT 0;

DECLARE
  source_table_name STRING;

DECLARE
  YEAR STRING;

DECLARE
  MONTH STRING;

DECLARE
  select_sql STRING;

DECLARE
  insert_sql STRING;

-- Read target table schema (column + datatype)
DECLARE target_schema ARRAY<STRUCT<column_name STRING, data_type STRING>>;

SET target_schema = (
  SELECT ARRAY_AGG(STRUCT(column_name, data_type) ORDER BY ordinal_position)
  FROM `pcb-{env}-landing.domain_scoring.INFORMATION_SCHEMA.COLUMNS`
  WHERE
    table_name = 'SCORING_PREP_PCMC_DATA_DEPOT'  -- target table
    AND column_name NOT IN ('YEAR', 'MONTH', 'REC_LOAD_TIMESTAMP', 'JOB_ID')
);

-- Collect table names
SET source_tables = (
  SELECT
    ARRAY_AGG(table_name)
  FROM
    `pcb-{deploy-env}-creditrisk.PCMC_DATA_DEPOT_MONTHLY.INFORMATION_SCHEMA.TABLES`
  WHERE
    table_name LIKE 'PCMC_DD_%'
);
-- Loop over array indexes
SET idx = 0;

WHILE
  idx < ARRAY_LENGTH(source_tables) DO
SET
  source_table_name = source_tables[
    OFFSET(idx)];

-- Extract YYYY and MM from table name
SET YEAR = REGEXP_EXTRACT(source_table_name, r'(\d{4})');
SET MONTH = SUBSTR(REGEXP_EXTRACT(source_table_name, r'(\d{6})'), 5, 2);
-- Build the SELECT list dynamically

SET select_sql = (
  SELECT
    STRING_AGG(
      IF(
        (
          SELECT COUNT(*)
          FROM
            `pcb-{deploy-env}-creditrisk.PCMC_DATA_DEPOT_MONTHLY.INFORMATION_SCHEMA.COLUMNS`
              info_schema_column
          WHERE
            info_schema_column.table_name = source_table_name
            AND info_schema_column.column_name = target_column.column_name
        ) > 0,
        -- Column exists → CAST(source_col AS target_datatype)
        FORMAT(
          "CAST(%s AS %s) AS %s",
          target_column.column_name,
          target_column.data_type,
          target_column.column_name),
        -- Column missing → CAST(NULL AS target_datatype)
        FORMAT(
          "CAST(NULL AS %s) AS %s",
          target_column.data_type,
          target_column.column_name)))
  FROM UNNEST(target_schema) AS target_column
);

-- Build SQL
SET
  insert_sql = FORMAT(
    """
    INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_PCMC_DATA_DEPOT`
    (
      MAST_ACCOUNT_ID,
      BILLING_CYC,
      CRLN,
      CRLN_CHG_TYPE,
      OPEN_YR,
      OPEN_MTH,
      ACQ_STGY_CD,
      MTHEND_PD_STAT,
      BK_TYPE,
      BK_IND,
      CO_IND,
      FR_IND,
      B_IND,
      CRVK_IND,
      CLOSED_IND,
      CRVK_DT,
      CLOSED_DT,
      SEC_FRD_IND,
      AM00_STATF_FRAUD,
      SYS_ACTIVE,
      AM00_CLIENT_PRODUCT_CODE,
      AM00_TRIAD_SCORE_ALIGNED,
      AM00_TRIAD_SCORE_RAW,
      AM00_STATC_DISPUTE,
      AM00_TYPEC_VIP,
      AM00_CUSTOM_DATA_81,
      ACCOUNT_NUMBER,
      AM01_PRIM_CARD_ID,
      MTHEND_BAL,
      LST_CRED_DT,
      B_AMT,
      B_DT,
      MTHEND_BAL_SUFFIX,
      MTHEND_CNT_SUFFIX,
      POSTAL_FSA,
      STATE,
      REGION,
      STMT_BAL,
      STMT_CRLN,
      TOT_DUE,
      CYC_PYMT,
      CYC_PYMT_NET,
      ANN_RATE,
      ACT_ANN_RATE,
      PREV_STMT_BAL,
      STMT_DUE_DT,
      STMT_PD_STAT,
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
      CYC_MRCH,
      CYC_CASH,
      FR_AMT,
      FR_CNT,
      BK_AMT_SUFFIX,
      BK_CNT_SUFFIX,
      CO_AMT_SUFFIX,
      CO_CNT_SUFFIX,
      CHGOFF_DT,
      COFF_RSN_CD,
      BK_AMT,
      CO_AMT,
      ORIG_CHGOFF_AMT,
      BNI_SCORE,
      ORIG_BNI_SCORE,
      SCORE_DT,
      CB_SCORE,
      ORIG_CB_SCORE,
      OFFER_LIMIT,
      CURR_LIMIT,
      OFFER_DT,
      OFFER,
      BEHV_SCORE_RAW,
      CO_REC,
      BK_REC,
      STATUS_CD,
      TOB,
      DISCL_GRP,
      APR_CODE,
      TOB_RANGE,
      CB_SCR_RANGE,
      ACCT_KEY,
      TEST_DIGIT,
      ORIG_CRLN,
      PREV_MTHEND_BAL,
      PREV_MTHEND_BAL_SUFFIX,
      PREV_CB_SCORE_6MTH,
      PREV_CB_SCORE_9MTH,
      PREV_CB_SCORE_12MTH,
      ACTIVE_IND,
      TRANS_IND,
      RVLVR_IND,
      DELQ_CNT_6MTH,
      ACTIVE_CNT_6MTH,
      TRANS_CNT_6MTH,
      RVLVR_CNT_6MTH,
      TOT_INT_6MTH,
      AVG_BAL_6MTH,
      DELQ_CNT_3MTH,
      ACTIVE_CNT_3MTH,
      TRANS_CNT_3MTH,
      RVLVR_CNT_3MTH,
      PROD_CD,
      CB_GRP,
      PD_STATUS,
      STMT_PD_STATUS,
      BAL,
      CLI_CTGY,
      ABP_INCOME,
      IDREST_INCOME,
      CRS_INCOME,
      RSASST_INCOME,
      CREDALRT_INCOME,
      FEES,
      LOYALTY,
      COF,
      OPEX,
      PROFIT,
      OPEN_DT,
      PROCESS_DT,
      SEC_FRD_DT,
      LST_MRCH_DT,
      LST_CASH_DT,
      LST_PYMT_DT,
      FST_ACTV_DT,
      DOB,
      YEAR,
      MONTH,
      REC_LOAD_TIMESTAMP,
      JOB_ID
    )
    SELECT %s,
      '%s' AS YEAR,
      '%s' AS MONTH,
      CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
      '{dag_id}' AS JOB_ID
    FROM `pcb-{deploy-env}-creditrisk.PCMC_DATA_DEPOT_MONTHLY.%s`
  """,
    select_sql,
    YEAR,
    MONTH,
    source_table_name
  );

-- Execute dynamic SQL
EXECUTE IMMEDIATE insert_sql;

-- Increment index
SET
  idx = idx + 1;

END WHILE;