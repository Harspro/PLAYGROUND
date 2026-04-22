BEGIN
  DECLARE report_month DATE;

DECLARE month_list ARRAY<STRING>;
DECLARE month_list_1 STRING;
DECLARE month_list_2 STRING;
DECLARE month_list_3 STRING;
DECLARE month_list_4 STRING;
DECLARE month_list_5 STRING;

SET report_month = (
  SELECT (get_report_month).report_month
  FROM
    (
      SELECT
        `pcb-{env}-landing.domain_scoring.get_report_month`(
          '{report_year}', '{report_month}') AS get_report_month
    )
);

SET month_list = (
  SELECT (get_report_month).month_list
  FROM
    (
      SELECT
        `pcb-{env}-landing.domain_scoring.get_report_month`(
          '{report_year}', '{report_month}') AS get_report_month
    )
);

SET month_list_1 = COALESCE(REPLACE(month_list[SAFE_OFFSET(1)], '-', ''), '99991201');
SET month_list_2 = COALESCE(REPLACE(month_list[SAFE_OFFSET(2)], '-', ''), '99991201');
SET month_list_3 = COALESCE(REPLACE(month_list[SAFE_OFFSET(3)], '-', ''), '99991201');
SET month_list_4 = COALESCE(REPLACE(month_list[SAFE_OFFSET(4)], '-', ''), '99991201');
SET month_list_5 = COALESCE(REPLACE(month_list[SAFE_OFFSET(5)], '-', ''), '99991201');

EXECUTE
  IMMEDIATE
    FORMAT(
      """
INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_HIST_6MTH`
(
  ACCT_KEY,
  DELQ_CNT_6MTH,
  ACTIVE_CNT_6MTH,
  TRANS_CNT_6MTH,
  RVLVR_CNT_6MTH,
  TOT_INT_6MTH,
  AVG_BAL_6MTH,
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
WITH WK_DATA_6MTH AS (
SELECT DISTINCT
  ACCT_KEY AS ACCT_KEY,
  PROCESS_DT AS PROCESS_DT,
  CAST(STMT_PD_STAT AS FLOAT64) AS STMT_PD_STAT,
  CAST(ACTIVE_IND AS FLOAT64) AS ACTIVE_IND,
  CAST(TRANS_IND AS FLOAT64) AS TRANS_IND,
  CAST(RVLVR_IND AS FLOAT64) AS RVLVR_IND,
  CAST(MTHEND_INT AS FLOAT64) AS MTHEND_INT,
  CAST(MTHEND_BAL AS FLOAT64) AS MTHEND_BAL
FROM
  `pcb-{env}-curated.domain_scoring.SCORING_PREP_WK_MTH6_LATEST`
UNION ALL
SELECT DISTINCT
  ACCT_KEY AS ACCT_KEY,
  PROCESS_DT AS PROCESS_DT,
  CAST(NULLIF(CAST(STMT_PD_STAT AS STRING), '0') AS FLOAT64) AS STMT_PD_STAT,
  CAST(NULLIF(CAST(ACTIVE_IND AS STRING), '0') AS FLOAT64) AS ACTIVE_IND,
  CAST(NULLIF(CAST(TRANS_IND AS STRING), '0') AS FLOAT64) AS TRANS_IND,
  CAST(NULLIF(CAST(RVLVR_IND AS STRING), '0') AS FLOAT64) AS RVLVR_IND,
  CAST(NULLIF(CAST(MTHEND_INT AS STRING), '0') AS FLOAT64) AS MTHEND_INT,
  CAST(NULLIF(CAST(MTHEND_BAL AS STRING), '0') AS FLOAT64) AS MTHEND_BAL
FROM
  `pcb-{env}-curated.domain_scoring.SCORING_PREP_PCMC_DATA_DEPOT_ALL` AS hist_month_list
   QUALIFY DENSE_RANK() OVER(PARTITION BY MAST_ACCOUNT_ID, YEAR, MONTH ORDER BY REC_LOAD_TIMESTAMP DESC) = 1
   AND CONCAT(hist_month_list.YEAR,hist_month_list.MONTH) IN ( SUBSTR('%s', 1, 6), SUBSTR('%s', 1, 6), SUBSTR('%s', 1, 6), SUBSTR('%s', 1, 6), SUBSTR('%s', 1, 6))
),
WK_HIST_6MTH_AGG AS (
SELECT DISTINCT
  wk_data_6mth.ACCT_KEY AS ACCT_KEY,
  COUNT(wk_data_6mth.STMT_PD_STAT) AS DELQ_CNT_6MTH,
  SUM(wk_data_6mth.ACTIVE_IND) AS ACTIVE_CNT_6MTH,
  SUM(wk_data_6mth.TRANS_IND) AS TRANS_CNT_6MTH,
  SUM(wk_data_6mth.RVLVR_IND) AS RVLVR_CNT_6MTH,
  SUM(wk_data_6mth.MTHEND_INT) AS TOT_INT_6MTH,
  CASE
    WHEN SUM(wk_data_6mth.ACTIVE_IND) > 0
      THEN
        SUM(wk_data_6mth.MTHEND_BAL)
        / SUM(wk_data_6mth.ACTIVE_IND)
    END AS AVG_BAL_6MTH
FROM
  WK_DATA_6MTH AS wk_data_6mth
GROUP BY
  wk_data_6mth.ACCT_KEY
)
SELECT DISTINCT
  wk_hist_6mth_agg.ACCT_KEY AS ACCT_KEY,
  CAST(wk_hist_6mth_agg.DELQ_CNT_6MTH AS STRING) AS DELQ_CNT_6MTH,
  CAST(wk_hist_6mth_agg.ACTIVE_CNT_6MTH AS STRING) AS ACTIVE_CNT_6MTH,
  CAST(wk_hist_6mth_agg.TRANS_CNT_6MTH AS STRING) AS TRANS_CNT_6MTH,
  CAST(wk_hist_6mth_agg.RVLVR_CNT_6MTH AS STRING) AS RVLVR_CNT_6MTH,
  CAST(wk_hist_6mth_agg.TOT_INT_6MTH AS STRING) AS TOT_INT_6MTH,
  CAST(wk_hist_6mth_agg.AVG_BAL_6MTH AS STRING) AS AVG_BAL_6MTH,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  WK_HIST_6MTH_AGG AS wk_hist_6mth_agg
""",
      month_list_1,
      month_list_2,
      month_list_3,
      month_list_4,
      month_list_5);

END
