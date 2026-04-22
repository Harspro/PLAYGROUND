-- Pull BCN and BNI scores accounts to populate pcmc data depot
DECLARE report_month DATE;

SET report_month = (
  SELECT (get_report_month).report_month
  FROM
    (
      SELECT
        `pcb-{env}-landing.domain_scoring.get_report_month`(
          '{report_year}', '{report_month}') AS get_report_month
    )
);

INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_TSYS_{score_name}_SCORE`
(
  MAST_ACCOUNT_ID,
  PROCESS_DT,
  SCORE_DT,
  {score_name}_score,
  SCORE_TYPE,
  orig_{score_name}_score,
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
WITH
  WK_TSYS_MTHEND_PREPARED AS (
  SELECT DISTINCT
    CAST(mast_account_id AS STRING) AS mast_account_id_str,
    mast_account_id,
    SAFE_CAST(
      PARSE_NUMERIC(NULLIF(AM00_CUSTOM_DATA_81, ""))
      AS STRING) AS app_num_str,
    AM00_APPLICATION_SUFFIX,
    open_dt,
    process_dt
  FROM `pcb-{env}-curated.domain_scoring.SCORING_PREP_WK_TSYS_MTHEND_FULL_LATEST`
  WHERE AM00_APPLICATION_SUFFIX = '0'
),
  WK_TSYS_SCORE1 AS (
    SELECT DISTINCT
      CAST(mast_account_id AS STRING) AS mast_account_id,
      sc01_score_aligned AS {score_name}_score,
      sc01_score_type AS score_type,
      DATE(sc01_date_score_added) AS score_dt,
      DATE_TRUNC(file_create_dt, MONTH) AS process_dt,
      CASE
        WHEN sc01_score_type = '{score_app_type}' THEN sc01_score_aligned
        END
        AS orig_{score_name}_score
    FROM
      `pcb-{env}-curated.domain_account_management.SC01`
    WHERE
      file_create_dt = report_month
      AND sc00_application_suffix = 0
      AND sc01_score_type IN (
        '{score_type}',
        '{score_app_type}')
      AND sc01_customer_type = 0
  ),
  WK_TSYS_SCORE_MISSING2 AS (
    SELECT DISTINCT
      wk_score_lnk_tbl.*,
      wk_tsys1.{score_name}_score,
      wk_tsys1.score_type,
      wk_tsys1.score_dt,
      wk_tsys1.process_dt,
      wk_tsys1.orig_{score_name}_score
    FROM
      `pcb-{env}-curated.domain_scoring.SCORING_PREP_WK_SCORE_LNK_LATEST`
        wk_score_lnk_tbl
    LEFT JOIN
      WK_TSYS_SCORE1 wk_tsys1
      ON
        wk_score_lnk_tbl.mast_account_id = wk_tsys1.mast_account_id
        AND wk_tsys1.score_type = '{score_app_type}'
    WHERE
      wk_tsys1.mast_account_id IS NULL
      AND SAFE_CAST(wk_score_lnk_tbl.orgn_{score_name}_score AS NUMERIC) >= 0
  ),
  WK_ADM AS (
    SELECT DISTINCT
      adm_std_dt_elemt.apa_app_num,
      CASE
        WHEN ele_element_type = 'N' THEN ele_numeric_value / 100000
        ELSE NULL
        END
        AS {score_name}_score,
      adm_std_dt_elemt.execution_id,
      adm_std_head_trail.file_create_dt AS ADM_EXTRACT_DT,
      PARSE_DATE("%Y%m%d", CAST(adm_std_app_data.apa_date_status_yyyymmdd AS STRING)) AS apa_date_status_yyyymmdd
    FROM
      `pcb-{env}-curated.domain_customer_acquisition.ADM_STD_DISP_DT_ELMT`
        adm_std_dt_elemt
    LEFT JOIN
      `pcb-{env}-curated.domain_customer_acquisition.ADM_STD_DISP_APP_DATA`
        adm_std_app_data
      ON
        adm_std_dt_elemt.apa_app_num = adm_std_app_data.apa_app_num
        AND adm_std_dt_elemt.execution_id = adm_std_app_data.execution_id
    LEFT JOIN
      `pcb-{env}-curated.domain_customer_acquisition.ADM_STD_DISP_HEAD_TRAIL`
        adm_std_head_trail
      ON
        adm_std_dt_elemt.execution_id = adm_std_head_trail.execution_id
    WHERE
      adm_std_dt_elemt.ele_element_name = '{score_app_name}'
      AND adm_std_app_data.apa_strategy = 'PCBSTRAT'
  ),
  WK_ADM_SCORE AS (
  SELECT DISTINCT
    CAST(mast_account_id AS STRING) AS mast_account_id,
    apa_app_num AS app_number,
    apa_date_status_yyyymmdd AS open_dt,
    CAST({score_name}_score AS STRING) AS orgn_{score_name}_score
  FROM
    (
      SELECT DISTINCT
        wk_adm.apa_app_num,
        wk_adm.{score_name}_score,
        wk_adm.apa_date_status_yyyymmdd,
        wk_tsys_mthend_full.mast_account_id,
        ROW_NUMBER()
          OVER (
            PARTITION BY wk_adm.apa_app_num
            ORDER BY wk_adm.ADM_EXTRACT_DT DESC
          ) AS newest
      FROM WK_ADM wk_adm
      LEFT JOIN WK_TSYS_MTHEND_PREPARED wk_tsys_mthend_full
        ON SAFE_CAST(wk_adm.apa_app_num AS STRING) = wk_tsys_mthend_full.app_num_str
        AND wk_tsys_mthend_full.open_dt >= '2010-06-26'
    )
  WHERE
    newest = 1
    AND apa_date_status_yyyymmdd > '2015-03-28'
),
  WK_TSYS_SCORE_MISSING_ADM AS (
    SELECT DISTINCT
      wk_adm_score.*,
      wk_tsys1.{score_name}_score,
      wk_tsys1.score_type,
      wk_tsys1.score_dt,
      wk_tsys1.process_dt,
      wk_tsys1.orig_{score_name}_score
    FROM
      WK_ADM_SCORE wk_adm_score
    LEFT JOIN
      WK_TSYS_SCORE1 wk_tsys1
      ON
        wk_adm_score.mast_account_id = wk_tsys1.mast_account_id
        AND wk_tsys1.score_type = '{score_app_type}'
    WHERE
      wk_tsys1.mast_account_id IS NULL
      AND SAFE_CAST(wk_adm_score.orgn_{score_name}_score AS NUMERIC) >= 0
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
      mast_account_id,
      process_dt,
      score_dt,
      {score_name}_score,
      score_type,
      orgn_{score_name}_score
    FROM
      WK_TSYS_SCORE_MISSING_ADM
  ),
WK_TSYS_SCORE_MISSING AS (
  SELECT DISTINCT
    wk_score_pull.mast_account_id,
    wk_score_pull.orgn_{score_name}_score AS orig_{score_name}_score,
    wk_score_pull.orgn_{score_name}_score AS {score_name}_score,
    wk_tsys_mtend.process_dt,
    wk_tsys_mtend.open_dt AS score_dt,
    '{score_app_type}' AS score_type
  FROM WK_SCORE_PULL wk_score_pull
  INNER JOIN WK_TSYS_MTHEND_PREPARED wk_tsys_mtend
    ON wk_score_pull.mast_account_id = wk_tsys_mtend.mast_account_id_str
),
  wk_tsys_score2 AS (
    SELECT DISTINCT
      mast_account_id,
      FORMAT_DATE('%Y-%m-%d', score_dt) AS score_dt,
      FORMAT_DATE('%Y-%m-%d', process_dt) AS process_dt,
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
  WK_TSYS_SCORE2_COMBINED AS (
    SELECT DISTINCT
      mast_account_id,
      process_dt,
      score_dt,
      {score_name}_score,
      score_type,
      orig_{score_name}_score,
      ROW_NUMBER()
        OVER (PARTITION BY mast_account_id ORDER BY score_dt DESC)
        AS newest_score,
      MAX(score_dt)
        OVER (PARTITION BY mast_account_id, process_dt) AS max_score_dt,
      MAX(orig_{score_name}_score)
        OVER (PARTITION BY mast_account_id, process_dt)
        AS max_orig_{score_name}_score
    FROM
      WK_TSYS_SCORE2
  )
SELECT DISTINCT
  mast_account_id AS MAST_ACCOUNT_ID,
  process_dt AS PROCESS_DT,
  max_score_dt AS SCORE_DT,
  {score_name}_score,
  score_type AS SCORE_TYPE,
  max_orig_{score_name}_score AS orig_{score_name}_score,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM
  WK_TSYS_SCORE2_COMBINED
WHERE
  newest_score = 1;