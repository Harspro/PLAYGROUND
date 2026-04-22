BEGIN
  DECLARE report_month DATE;

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

INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_TSYS_AM01`
(
  FILE_CREATE_DT,
  MAST_ACCOUNT_ID,
  AM00_APPLICATION_SUFFIX,
  ACCOUNT_NUMBER,
  AM01_PRIM_CARD_ID,
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
SELECT DISTINCT
  CAST(file_create_dt AS STRING) AS FILE_CREATE_DT,
  CAST(mast_account_id AS STRING) AS MAST_ACCOUNT_ID,
  CAST(am00_application_suffix AS STRING) AS AM00_APPLICATION_SUFFIX,
  CAST(am01_account_num AS STRING) AS ACCOUNT_NUMBER,
  am01_prim_card_id AS AM01_PRIM_CARD_ID,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  `pcb-{env}-curated.domain_account_management.AM01`
WHERE
  file_create_dt = report_month
  AND am01_customer_type = 0
  AND COALESCE(am01_account_rel_stat, '') IN ('A', '');

END
