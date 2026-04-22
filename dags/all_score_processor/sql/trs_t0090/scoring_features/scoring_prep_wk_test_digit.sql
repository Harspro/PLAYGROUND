BEGIN
  DECLARE month_list ARRAY<STRING>;

DECLARE month_list_0 STRING;

SET month_list = (
  SELECT (get_report_month).month_list
  FROM
    (
      SELECT
        `pcb-{env}-landing.domain_scoring.get_report_month`(
          '{report_year}', '{report_month}') AS get_report_month
    )
);

SET month_list_0 = REPLACE(month_list[SAFE_OFFSET(0)], '-', '');

INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_TEST_DIGIT`
(
  FILE_CREATE_DT,
  PROCESS_DT,
  MAST_ACCOUNT_ID,
  SPID,
  TEST_DIGIT,
  REPORT_DATE,
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
SELECT DISTINCT
  CAST(cif_card_curr.FILE_CREATE_DT AS STRING) AS FILE_CREATE_DT,
  CAST(DATE_TRUNC(cif_card_curr.FILE_CREATE_DT, MONTH) AS STRING) AS PROCESS_DT,
  CAST(cif_card_curr.CIFP_ACCOUNT_ID6 AS STRING) AS MAST_ACCOUNT_ID,
  CAST(cif_card_curr.CIFP_SPID_TYPE6 AS STRING) AS SPID,
  CAST(cif_card_curr.CIFP_TEST_DIGIT_TYPE6 AS STRING) AS TEST_DIGIT,
  CAST(DATE(PARSE_DATE('%Y%m%d', month_list_0)) AS STRING) AS REPORT_DATE,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_account_management.CIF_CARD_CURR` AS cif_card_curr
WHERE
  cif_card_curr.CIFP_CUSTOMER_TYPE = 0
  AND (
    cif_card_curr.CIFP_RELATIONSHIP_STAT = 'A'
    OR cif_card_curr.CIFP_RELATIONSHIP_STAT IS NULL
    OR cif_card_curr.CIFP_RELATIONSHIP_STAT
      = '');

END