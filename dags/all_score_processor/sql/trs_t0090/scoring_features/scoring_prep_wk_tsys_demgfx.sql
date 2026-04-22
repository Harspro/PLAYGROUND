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

INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_TSYS_DEMGFX`
(
  MAST_ACCOUNT_ID,
  AM00_APPLICATION_SUFFIX,
  POSTAL_FSA,
  DOB,
  STATE,
  REGION,
  PROCESS_DT,
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
WITH
  AM0E AS (
    SELECT DISTINCT
      am0e.MAST_ACCOUNT_ID,
      am0e.AM00_APPLICATION_SUFFIX,
      am0e.AM0E_ZIP_POSTAL_CODE,
      CASE
        WHEN LENGTH(am0e.AM0E_ZIP_POSTAL_CODE) >= 3
          THEN SUBSTR(am0e.AM0E_ZIP_POSTAL_CODE, 1, 3)
        ELSE NULL
        END AS POSTAL_FSA,
      am0e.AM0E_DATE_BIRTH,
      TRIM(am0e.AM0E_STATE_PROV_CODE) AS STATE,
      DATE_TRUNC(am0e.FILE_CREATE_DT, MONTH) AS PROCESS_DT,
      am0e.FILE_CREATE_DT,
      am0e.AM0E_CUSTOMER_TYPE
    FROM `pcb-{env}-curated.domain_account_management.AM0E` AS am0e
    WHERE
      DATE_TRUNC(am0e.FILE_CREATE_DT, MONTH) = DATE_TRUNC(report_month, MONTH)
      AND am0e.AM00_APPLICATION_SUFFIX = 0
      AND am0e.AM0E_CUSTOMER_TYPE = 0
  )
SELECT DISTINCT
  CAST(am0e.MAST_ACCOUNT_ID AS STRING) AS MAST_ACCOUNT_ID,
  CAST(am0e.AM00_APPLICATION_SUFFIX AS STRING) AS AM00_APPLICATION_SUFFIX,
  am0e.POSTAL_FSA AS POSTAL_FSA,
  CAST(DATE(am0e.AM0E_DATE_BIRTH) AS STRING) AS DOB,
  am0e.STATE AS STATE,
  CASE
    WHEN am0e.STATE IN ('AB', 'BC', 'SK', 'MB', 'NT', 'NU', 'YT') THEN 'WEST'
    WHEN am0e.STATE IN ('PE', 'NB', 'NS', 'NL') THEN 'EAST'
    WHEN am0e.STATE = 'QC' AND SUBSTR(am0e.AM0E_ZIP_POSTAL_CODE, 1, 1) = 'H'
      THEN 'QC-MTL'
    WHEN am0e.STATE = 'QC' THEN 'QC'
    WHEN am0e.STATE = 'ON' AND SUBSTR(am0e.AM0E_ZIP_POSTAL_CODE, 1, 1) = 'M'
      THEN 'ON-GTA'
    WHEN am0e.STATE = 'ON' THEN 'ON'
    END
    AS REGION,
  CAST(DATE_TRUNC(am0e.FILE_CREATE_DT, MONTH) AS STRING) AS PROCESS_DT,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  AM0E AS am0e;

END