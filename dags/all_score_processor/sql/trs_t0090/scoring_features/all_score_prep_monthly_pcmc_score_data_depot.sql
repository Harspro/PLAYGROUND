-- Pull BCN and BNI scores accounts to populate pcmc data depot
DECLARE report_month DATE;

SET report_month = (
  SELECT (get_report_month).report_month
FROM (SELECT `pcb-{env}-landing.domain_scoring.get_report_month`('{report_year}', '{report_month}') AS get_report_month)
);

CREATE OR REPLACE TABLE `pcb-{env}-landing.domain_scoring.WK_TSYS_{score_name}_SCORE` AS
WITH WK_TSYS_SCORE1 AS (
SELECT DISTINCT
  mast_account_id,
  sc01_score_aligned AS {score_name}_score,
  sc01_score_type AS score_type,
  DATE(sc01_date_score_added) AS score_dt,
  DATE_TRUNC(file_create_dt, MONTH)  AS process_dt,
  CASE
    WHEN sc01_score_type='{score_app_type}' THEN sc01_score_aligned
END
  AS orig_{score_name}_score
FROM
  `pcb-{env}-curated.domain_account_management.SC01`
WHERE
  file_create_dt = report_month
  AND sc00_application_suffix=0
  AND sc01_score_type IN ('{score_type}',
    '{score_app_type}')
  AND sc01_customer_type=0
ORDER BY
  mast_account_id,
  score_dt DESC
),

WK_TSYS_SCORE_MISSING3 AS (
SELECT DISTINCT
  *
FROM
  WK_TSYS_SCORE1
WHERE
  score_type='{score_app_type}'
ORDER BY
  mast_account_id
),

WK_TSYS_SCORE_MISSING2 AS (
SELECT DISTINCT
  experian_lnk_tbl.*,
  {score_name}_score,
  score_type,
  score_dt,
  process_dt,
  orig_{score_name}_score
FROM
  `pcb-{env}-landing.domain_scoring.WK_EXPERIAN_SCORE` experian_lnk_tbl
LEFT JOIN
  WK_TSYS_SCORE_MISSING3 wl_tsys_missing3
ON
  SAFE_CAST(experian_lnk_tbl.mast_account_id AS STRING) = SAFE_CAST(wl_tsys_missing3.mast_account_id AS STRING)
WHERE
  wl_tsys_missing3.mast_account_id IS NULL
  AND SAFE_CAST(experian_lnk_tbl.orgn_{score_name}_score AS NUMERIC)>=0
ORDER BY
  experian_lnk_tbl.mast_account_id
),

WK_ADM AS (
SELECT DISTINCT
  adm_std_dt_elemt.apa_app_num,
  CASE
    WHEN ele_element_type='N' THEN ele_numeric_value/100000
    ELSE NULL
END
  AS {score_name}_score,
  adm_std_dt_elemt.execution_id,
  adm_std_head_trail.file_create_dt AS ADM_EXTRACT_DT,
  adm_std_app_data.apa_date_status_yyyymmdd
FROM
  `pcb-{env}-curated.domain_customer_acquisition.ADM_STD_DISP_DT_ELMT` adm_std_dt_elemt
LEFT JOIN
  `pcb-{env}-curated.domain_customer_acquisition.ADM_STD_DISP_APP_DATA` adm_std_app_data
ON
  adm_std_dt_elemt.apa_app_num=adm_std_app_data.apa_app_num
  AND adm_std_dt_elemt.execution_id=adm_std_app_data.execution_id
LEFT JOIN
  `pcb-{env}-curated.domain_customer_acquisition.ADM_STD_DISP_HEAD_TRAIL` adm_std_head_trail
ON
  adm_std_dt_elemt.execution_id=adm_std_head_trail.execution_id
WHERE
  adm_std_dt_elemt.ele_element_name='{score_app_name}'
  AND adm_std_app_data.apa_strategy='PCBSTRAT'
ORDER BY
  adm_std_dt_elemt.apa_app_num,
  adm_std_head_trail.file_create_dt DESC
),

WK_ADM1 AS (
SELECT DISTINCT
  wk_adm.*,
  wk_mthend_id.mast_account_id
FROM
  WK_ADM wk_adm
LEFT JOIN
  `pcb-{env}-landing.domain_scoring.WK_MTHEND_ID` wk_mthend_id
ON
  SAFE_CAST(wk_adm.apa_app_num AS STRING)=SAFE_CAST(wk_mthend_id.app_number AS STRING)
ORDER BY
  wk_adm.apa_app_num,
  wk_adm.ADM_EXTRACT_DT DESC
),

WK_ADM2 AS (
SELECT DISTINCT
  *,
  PARSE_DATE("%Y%m%d", CAST(apa_date_status_yyyymmdd AS string)) AS status_date
FROM (
  SELECT DISTINCT
    *,
    ROW_NUMBER() OVER (PARTITION BY apa_app_num ORDER BY ADM_EXTRACT_DT DESC) AS newest
  FROM
    WK_ADM1 )
WHERE
  newest = 1
ORDER BY
  mast_account_id
),

WK_ADM_SCORE AS ( -- do we need to save as wk_admM3 also
SELECT DISTINCT
  mast_account_id,
  apa_app_num AS app_number,
  status_date AS open_dt,
  {score_name}_score AS orgn_{score_name}_score
FROM
  WK_ADM2
WHERE
  status_date > '2015-03-28'
ORDER BY
  mast_account_id
),

WK_TSYS_SCORE_MISSING_ADM AS (
SELECT DISTINCT
  wk_adm_score.*,
  wk_tsys_missing3.* EXCEPT(mast_account_id )
FROM
  WK_ADM_SCORE wk_adm_score
LEFT JOIN
  WK_TSYS_SCORE_MISSING3 wk_tsys_missing3
ON
  SAFE_CAST(wk_adm_score.mast_account_id AS STRING) = SAFE_CAST(wk_tsys_missing3.mast_account_id AS STRING)
WHERE
  wk_tsys_missing3.mast_account_id IS NULL
  AND SAFE_CAST(wk_adm_score.orgn_{score_name}_score AS NUMERIC) >= 0
ORDER BY
  wk_adm_score.mast_account_id
),
  -- /*Accounts found scores in Acquisition2 and ADM*/
WK_SCORE_PULL AS (
SELECT DISTINCT
  mast_account_id,
  process_dt,
  score_dt,
  {score_name}_score,
  score_type,
  orgn_{score_name}_score
FROM
  WK_TSYS_SCORE_MISSING2
UNION ALL
SELECT DISTINCT
  CAST(mast_account_id AS string) AS mast_account_id,
  process_dt,
  score_dt,
  {score_name}_score,
  score_type,
  CAST(orgn_{score_name}_score AS string) as orgn_{score_name}_score
FROM
  WK_TSYS_SCORE_MISSING_ADM
ORDER BY
  mast_account_id
),

WK_TSYS_SCORE_MISSING AS (
SELECT DISTINCT
  wk_score_pull.mast_account_id,
  wk_score_pull.orgn_{score_name}_score AS orig_{score_name}_score,
  wk_score_pull.orgn_{score_name}_score AS {score_name}_score,
  wk_tsys_mtend.process_dt,
  wk_tsys_mtend.open_dt AS score_dt,
  '{score_app_type}' AS score_type
FROM
  wk_Score_pull wk_score_pull
INNER JOIN
  `pcb-{env}-landing.domain_scoring.WK_TSYS_MTHEND` wk_tsys_mtend
ON
  SAFE_CAST(wk_score_pull.mast_account_id AS STRING) = SAFE_CAST(wk_tsys_mtend.mast_account_id AS STRING)
ORDER BY
  wk_score_pull.mast_account_id
),

wk_tsys_score2 AS (
SELECT DISTINCT
  CAST(mast_account_id AS string) AS mast_account_id,
  score_dt,
  process_dt,
  CAST({score_name}_score AS string) AS {score_name}_score,
  score_type,
  CAST(orig_{score_name}_score AS string) AS orig_{score_name}_score
FROM
  WK_TSYS_SCORE1
UNION ALL
SELECT DISTINCT
  mast_account_id,
  score_dt,
  process_dt,
  {score_name}_score,
  score_type,
  orig_{score_name}_score
FROM
  WK_TSYS_SCORE_MISSING
),

WK_TSYS_SCORE2_T80 AS (
SELECT DISTINCT
  mast_account_id,
  process_dt,
  MAX(score_dt) AS score_dt,
  MAX(orig_{score_name}_score) AS orig_{score_name}_score
FROM
  WK_TSYS_SCORE2
GROUP BY
  mast_account_id,
  process_dt
ORDER BY
  mast_account_id,
  process_dt
),

WK_TSYS_SCORE2_NODUP AS (
SELECT DISTINCT
  *
FROM (
  SELECT DISTINCT
    *,
    ROW_NUMBER() OVER (PARTITION BY mast_account_id ORDER BY score_dt DESC) AS newest_score
  FROM
    WK_TSYS_SCORE2 )
WHERE
  newest_score = 1
ORDER BY
  mast_account_id,
  score_dt DESC
)

SELECT
  wk_tsys_score2_t80.mast_account_id AS MAST_ACCOUNT_ID ,
  wk_tsys_score2_t80.process_dt AS PROCESS_DT,
  wk_tsys_score2_t80.score_dt AS SCORE_DT,
  wk_score2_nodup.{score_name}_score ,
  wk_score2_nodup.score_type AS SCORE_TYPE,
  wk_tsys_score2_t80.orig_{score_name}_score
FROM
  wk_tsys_score2_nodup wk_score2_nodup
LEFT JOIN
  WK_TSYS_SCORE2_T80 wk_tsys_score2_t80
ON
  SAFE_CAST(wk_score2_nodup.mast_account_id AS STRING)=SAFE_CAST(wk_tsys_score2_t80.mast_account_id AS STRING)
ORDER BY
  wk_score2_nodup.mast_account_id;