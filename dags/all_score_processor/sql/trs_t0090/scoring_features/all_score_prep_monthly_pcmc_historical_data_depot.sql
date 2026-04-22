BEGIN

DECLARE report_month DATE;
DECLARE month_list ARRAY<STRING>;
DECLARE month_list_1 STRING;
DECLARE month_list_2 STRING;
DECLARE month_list_3 STRING;
DECLARE month_list_4 STRING;
DECLARE month_list_5 STRING;
DECLARE month_list_9 STRING;
DECLARE month_list_12 STRING;

SET report_month = (
  SELECT (get_report_month).report_month
FROM (SELECT `pcb-{env}-landing.domain_scoring.get_report_month`('{report_year}', '{report_month}') AS get_report_month)
);

SET month_list = (
  SELECT (get_report_month).month_list
FROM (SELECT `pcb-{env}-landing.domain_scoring.get_report_month`('{report_year}', '{report_month}') AS get_report_month)
);

SET month_list_1  = REPLACE(month_list[SAFE_OFFSET(1)],'-','');
SET month_list_2  = REPLACE(month_list[SAFE_OFFSET(2)],'-','');
SET month_list_3  = REPLACE(month_list[SAFE_OFFSET(3)],'-','');
SET month_list_4  = REPLACE(month_list[SAFE_OFFSET(4)],'-','');
SET month_list_5  = REPLACE(month_list[SAFE_OFFSET(5)],'-','');
SET month_list_9  = REPLACE(month_list[SAFE_OFFSET(9)],'-','');
SET month_list_12 = REPLACE(month_list[SAFE_OFFSET(12)],'-','');


EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE
  `pcb-{env}-landing.domain_scoring.WK_MTH6` AS
SELECT DISTINCT
  DISTINCT current_month.*,
  hist_month_list.CRLN AS ORIG_CRLN,
  hist_month_list.MTHEND_BAL AS PREV_MTHEND_BAL,
  hist_month_list.MTHEND_BAL_SUFFIX AS PREV_MTHEND_BAL_SUFFIX,
  hist_month_list.CB_SCORE AS PREV_CB_SCORE_6MTH,
  hist_month_list.CB_SCORE AS PREV_CB_SCORE_9MTH,
  hist_month_list.CB_SCORE AS PREV_CB_SCORE_12MTH
FROM
  `pcb-{env}-landing.domain_scoring.WK_MMM` AS current_month
LEFT JOIN
(SELECT DISTINCT * FROM  `pcb-{env}-landing.domain_scoring.SCORING_PREP_PCMC_DATA_DEPOT` 
 WHERE CONCAT(YEAR,MONTH) IN ( SUBSTR('%s', 1, 6), SUBSTR('%s', 1, 6), SUBSTR('%s', 1, 6), SUBSTR('%s', 1, 6))) AS hist_month_list
ON
  CAST(current_month.ACCT_KEY AS STRING) = hist_month_list.ACCT_KEY
 """, month_list_1,month_list_5,month_list_9,month_list_12);
  
EXECUTE IMMEDIATE FORMAT("""
CREATE TEMP TABLE WK_DATA_6MTH AS
SELECT DISTINCT
  CAST(ACCT_KEY AS STRING) AS ACCT_KEY,
  CAST(PROCESS_DT AS TIMESTAMP) AS PROCESS_DT,
  CAST(STMT_PD_STAT AS STRING) AS STMT_PD_STAT,
  CAST(ACTIVE_IND AS STRING) AS ACTIVE_IND,
  CAST(TRANS_IND AS STRING) AS TRANS_IND,
  CAST(RVLVR_IND AS STRING) AS RVLVR_IND,
  CAST(MTHEND_INT AS STRING) AS MTHEND_INT,
  CAST(MTHEND_BAL AS STRING) AS MTHEND_BAL
FROM
  `pcb-{env}-landing.domain_scoring.WK_MTH6`
UNION ALL
SELECT DISTINCT
  ACCT_KEY,
  CAST(PROCESS_DT AS TIMESTAMP) AS PROCESS_DT,
  NULLIF(CAST(STMT_PD_STAT AS STRING), '0') AS STMT_PD_STAT, 
  NULLIF(CAST(ACTIVE_IND AS STRING), '0') AS ACTIVE_IND, 
  NULLIF(CAST(TRANS_IND AS STRING), '0') AS TRANS_IND, 
  NULLIF(CAST(RVLVR_IND AS STRING), '0') AS RVLVR_IND,
  NULLIF(CAST(MTHEND_INT AS STRING), '0') AS MTHEND_INT,
  NULLIF(CAST(MTHEND_BAL AS STRING), '0') AS MTHEND_BAL 
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_PCMC_DATA_DEPOT` AS hist_month_list
WHERE CONCAT(hist_month_list.YEAR,hist_month_list.MONTH) IN ( SUBSTR('%s', 1, 6), SUBSTR('%s', 1, 6), SUBSTR('%s', 1, 6), SUBSTR('%s', 1, 6), SUBSTR('%s', 1, 6))
""", month_list_1,month_list_2,month_list_3,month_list_4,month_list_5);

EXECUTE IMMEDIATE FORMAT("""
CREATE TEMP TABLE WK_DATA_3MTH AS
SELECT DISTINCT
  CAST(ACCT_KEY AS STRING) AS ACCT_KEY,
  CAST(PROCESS_DT AS TIMESTAMP) AS PROCESS_DT,
  CAST(STMT_PD_STAT AS STRING) AS STMT_PD_STAT,
  CAST(ACTIVE_IND AS STRING) AS ACTIVE_IND,
  CAST(TRANS_IND AS STRING) AS TRANS_IND,
  CAST(RVLVR_IND AS STRING) AS RVLVR_IND
FROM
  `pcb-{env}-landing.domain_scoring.WK_MTH6`
UNION ALL
SELECT DISTINCT
  ACCT_KEY,
  CAST(PROCESS_DT AS TIMESTAMP) AS PROCESS_DT,
  STMT_PD_STAT,
  ACTIVE_IND,
  TRANS_IND,
  RVLVR_IND
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_PCMC_DATA_DEPOT` AS hist_month_list
WHERE CONCAT(hist_month_list.YEAR,hist_month_list.MONTH) IN ( SUBSTR('%s', 1, 6), SUBSTR('%s', 1, 6))
""", month_list_1,month_list_2);

CREATE OR REPLACE TABLE
  `pcb-{env}-landing.domain_scoring.WK_HIST_6MTH` AS
SELECT DISTINCT
  wk_data_6mth.ACCT_KEY,
  COUNT(CAST(wk_data_6mth.STMT_PD_STAT AS FLOAT64)) AS DELQ_CNT_6MTH,
  SUM(CAST(wk_data_6mth.ACTIVE_IND AS FLOAT64)) AS ACTIVE_CNT_6MTH,
  SUM(CAST(wk_data_6mth.TRANS_IND AS FLOAT64)) AS TRANS_CNT_6MTH,
  SUM(CAST(wk_data_6mth.RVLVR_IND AS FLOAT64)) AS RVLVR_CNT_6MTH,
  SUM(CAST(wk_data_6mth.MTHEND_INT AS FLOAT64)) AS TOT_INT_6MTH,
  CASE
    WHEN SUM(CAST(wk_data_6mth.ACTIVE_IND AS FLOAT64)) > 0 THEN SUM(CAST(wk_data_6mth.MTHEND_BAL AS FLOAT64))/SUM(CAST(wk_data_6mth.ACTIVE_IND AS FLOAT64))
END
  AS AVG_BAL_6MTH
FROM
  WK_DATA_6MTH AS wk_data_6mth
GROUP BY
  wk_data_6mth.ACCT_KEY;


CREATE OR REPLACE TABLE
  `pcb-{env}-landing.domain_scoring.WK_HIST_3MTH` AS
SELECT DISTINCT
  wk_data_3mth.ACCT_KEY,
  COUNT(CAST(wk_data_3mth.STMT_PD_STAT AS FLOAT64)) AS DELQ_CNT_3MTH,
  SUM(CAST(wk_data_3mth.ACTIVE_IND AS FLOAT64)) AS ACTIVE_CNT_3MTH,
  SUM(CAST(wk_data_3mth.TRANS_IND AS FLOAT64)) AS TRANS_CNT_3MTH,
  SUM(CAST(wk_data_3mth.RVLVR_IND AS FLOAT64)) AS RVLVR_CNT_3MTH
FROM
  WK_DATA_3MTH AS wk_data_3mth
GROUP BY
  wk_data_3mth.ACCT_KEY;

END
