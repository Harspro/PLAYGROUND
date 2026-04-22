BEGIN

DECLARE report_month DATE;
DECLARE month_list ARRAY<STRING>;
DECLARE month_list_0 STRING;

SET report_month = (
  SELECT (get_report_month).report_month
FROM (SELECT `pcb-{env}-landing.domain_scoring.get_report_month`('{report_year}', '{report_month}') AS get_report_month)
);

SET month_list = (
  SELECT (get_report_month).month_list
FROM (SELECT `pcb-{env}-landing.domain_scoring.get_report_month`('{report_year}', '{report_month}') AS get_report_month)
);

SET month_list_0 = REPLACE(month_list[SAFE_OFFSET(0)],'-','');


CREATE OR REPLACE TABLE
  `pcb-{env}-landing.domain_scoring.WK_TSYS_DEMGFX` AS

WITH AM0E AS (
  SELECT DISTINCT
    am0e.MAST_ACCOUNT_ID,
    am0e.AM00_APPLICATION_SUFFIX,
    am0e.AM0E_ZIP_POSTAL_CODE,
    SUBSTR(am0e.AM0E_ZIP_POSTAL_CODE, 1, 3) AS POSTAL_FSA,
    am0e.AM0E_DATE_BIRTH,
    TRIM(am0e.AM0E_STATE_PROV_CODE) AS STATE,
    DATE_TRUNC(am0e.FILE_CREATE_DT, MONTH) AS PROCESS_DT,
    am0e.FILE_CREATE_DT,
    am0e.AM0E_CUSTOMER_TYPE
  FROM `pcb-{env}-curated.domain_account_management.AM0E` AS am0e
)

SELECT DISTINCT
  am0e.MAST_ACCOUNT_ID,
  am0e.AM00_APPLICATION_SUFFIX,
  am0e.POSTAL_FSA,
  DATE(am0e.AM0E_DATE_BIRTH) AS DOB,
  am0e.STATE,
  CASE
    WHEN am0e.STATE IN ('AB', 'BC', 'SK', 'MB', 'NT', 'NU', 'YT') THEN 'WEST'
    WHEN am0e.STATE IN ('PE','NB','NS','NL') THEN 'EAST'
    WHEN am0e.STATE = 'QC' AND SUBSTR(am0e.AM0E_ZIP_POSTAL_CODE,1,1) = 'H' THEN 'QC-MTL'
    WHEN am0e.STATE = 'QC' THEN 'QC'
    WHEN am0e.STATE = 'ON' AND SUBSTR(am0e.AM0E_ZIP_POSTAL_CODE,1,1) = 'M' THEN 'ON-GTA'
    WHEN am0e.STATE = 'ON' THEN 'ON'
END
  AS REGION,
  DATE_TRUNC(am0e.FILE_CREATE_DT, MONTH) AS PROCESS_DT,
FROM
  AM0E AS am0e
WHERE
  DATE_TRUNC(am0e.FILE_CREATE_DT, MONTH) = DATE_TRUNC(report_month, MONTH)
  AND am0e.AM00_APPLICATION_SUFFIX=0
  AND am0e.AM0E_CUSTOMER_TYPE=0
ORDER BY
  am0e.MAST_ACCOUNT_ID,
  am0e.AM00_APPLICATION_SUFFIX ;

CREATE OR REPLACE TABLE
  `pcb-{env}-landing.domain_scoring.WK_TEST_DIGIT` AS
SELECT DISTINCT
  cif_card_curr.FILE_CREATE_DT,
  DATE_TRUNC(cif_card_curr.FILE_CREATE_DT, MONTH) AS PROCESS_DT,
  cif_card_curr.CIFP_ACCOUNT_ID6 AS MAST_ACCOUNT_ID,
  cif_card_curr.CIFP_SPID_TYPE6 AS SPID,
  cif_card_curr.CIFP_TEST_DIGIT_TYPE6 AS TEST_DIGIT
FROM
  `pcb-{env}-curated.domain_account_management.CIF_CARD_CURR` AS cif_card_curr
WHERE
  cif_card_curr.CIFP_CUSTOMER_TYPE=0
  AND (cif_card_curr.CIFP_RELATIONSHIP_STAT IN ('A')
    OR NULLIF(cif_card_curr.CIFP_RELATIONSHIP_STAT, "") IS NULL )
ORDER BY
  cif_card_curr.CIFP_ACCOUNT_ID6 ;

-- SAVE A TEST DIGIT TABLE FOR EVERY MONTH SO WE CAN REUSE WHEN WE RERUN PAST MONTH TABLES
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE
  `pcb-{env}-landing.domain_scoring.WK_TEST_DIGIT_%s` AS
SELECT DISTINCT
  *
FROM
  `pcb-{env}-landing.domain_scoring.WK_TEST_DIGIT`
  """, month_list_0);

END
