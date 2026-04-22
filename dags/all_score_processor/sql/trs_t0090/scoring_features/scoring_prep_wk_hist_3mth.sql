BEGIN
  DECLARE report_month DATE;

DECLARE month_list ARRAY<STRING>;
DECLARE month_list_1 STRING;
DECLARE month_list_2 STRING;

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

EXECUTE
  IMMEDIATE
    FORMAT(
      """
INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_HIST_3MTH`
(
  ACCT_KEY,
  DELQ_CNT_3MTH,
  ACTIVE_CNT_3MTH,
  TRANS_CNT_3MTH,
  RVLVR_CNT_3MTH,
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
WITH WK_DATA_3MTH AS (
SELECT DISTINCT
  ACCT_KEY AS ACCT_KEY,
  PROCESS_DT AS PROCESS_DT,
  CAST(STMT_PD_STAT AS FLOAT64) AS STMT_PD_STAT,
  CAST(ACTIVE_IND AS FLOAT64) AS ACTIVE_IND,
  CAST(TRANS_IND AS FLOAT64) AS TRANS_IND,
  CAST(RVLVR_IND AS FLOAT64) AS RVLVR_IND
FROM
  `pcb-{env}-curated.domain_scoring.SCORING_PREP_WK_MTH6_LATEST`
UNION ALL
SELECT DISTINCT
  ACCT_KEY AS ACCT_KEY,
  PROCESS_DT AS PROCESS_DT,
  CAST(STMT_PD_STAT AS FLOAT64) AS STMT_PD_STAT,
  CAST(ACTIVE_IND AS FLOAT64) AS ACTIVE_IND,
  CAST(TRANS_IND AS FLOAT64) AS TRANS_IND,
  CAST(RVLVR_IND AS FLOAT64) AS RVLVR_IND
FROM
  `pcb-{env}-curated.domain_scoring.SCORING_PREP_PCMC_DATA_DEPOT_ALL` AS hist_month_list
   QUALIFY DENSE_RANK() OVER(PARTITION BY MAST_ACCOUNT_ID, YEAR, MONTH ORDER BY REC_LOAD_TIMESTAMP DESC) = 1
   AND CONCAT(hist_month_list.YEAR,hist_month_list.MONTH) IN ( SUBSTR('%s', 1, 6), SUBSTR('%s', 1, 6))
),
WK_HIST_3MTH_AGG AS (
SELECT DISTINCT
  wk_data_3mth.ACCT_KEY AS ACCT_KEY,
  COUNT(wk_data_3mth.STMT_PD_STAT) AS DELQ_CNT_3MTH,
  SUM(wk_data_3mth.ACTIVE_IND) AS ACTIVE_CNT_3MTH,
  SUM(wk_data_3mth.TRANS_IND) AS TRANS_CNT_3MTH,
  SUM(wk_data_3mth.RVLVR_IND) AS RVLVR_CNT_3MTH
FROM
  WK_DATA_3MTH AS wk_data_3mth
GROUP BY
  wk_data_3mth.ACCT_KEY
)
SELECT DISTINCT
  wk_hist_3mth_agg.ACCT_KEY,
  CAST(wk_hist_3mth_agg.DELQ_CNT_3MTH AS STRING) AS DELQ_CNT_3MTH,
  CAST(wk_hist_3mth_agg.ACTIVE_CNT_3MTH AS STRING) AS ACTIVE_CNT_3MTH,
  CAST(wk_hist_3mth_agg.TRANS_CNT_3MTH AS STRING) AS TRANS_CNT_3MTH,
  CAST(wk_hist_3mth_agg.RVLVR_CNT_3MTH AS STRING) AS RVLVR_CNT_3MTH,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  WK_HIST_3MTH_AGG AS wk_hist_3mth_agg
""",
      month_list_1,
      month_list_2);

END
