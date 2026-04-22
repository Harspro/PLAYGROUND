-- Index4 T0040: Insert area risk lookup into Landing Table
-- This query computes area-level risk rankings by aggregating
-- postal code bad rates and applying the risk ranking formula

INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_AREA_RISK_LOOKUP`
(
  AREA_CD,
  AREA_RANK,
  TOTAL_ACCOUNTS,
  TOT_BADS,
  BAD_RATE,
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
SELECT DISTINCT
  CAST(area_cd AS STRING) AS AREA_CD,
  CAST(area_rank AS STRING) AS AREA_RANK,
  CAST(total_accounts AS STRING) AS TOTAL_ACCOUNTS,
  CAST(tot_bads AS STRING) AS TOT_BADS,
  CAST(SAFE_DIVIDE(tot_bads, total_accounts) AS STRING) AS BAD_RATE,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM (
  SELECT DISTINCT
    area_cd,
    total_accounts,
    tot_bads,
    CASE 
      -- Cap bad rate at 8% (area_rank = 99)
      WHEN SAFE_DIVIDE(tot_bads, total_accounts) > 0.08 THEN 99
      -- Floor at 0% (area_rank = 1)
      WHEN SAFE_DIVIDE(tot_bads, total_accounts) <= 0 THEN 1
      -- Scale bad rate by 0.0053 and round up
      ELSE CEILING(SAFE_DIVIDE(tot_bads, total_accounts) / 0.0053)
    END AS area_rank
  FROM (
    SELECT DISTINCT
      LEFT(CIFP_ZIP_CODE, 3) AS area_cd,
      COUNT(DISTINCT MAST_ACCOUNT_ID_T0) AS total_accounts,
      SUM(SAFE_CAST(BAD_FLAG_CYCLE AS INT64)) AS tot_bads
    FROM `pcb-{env}-curated.domain_scoring.SCORING_PREP_POSTAL_CODE_RI_V4_RISK_RANKING_LATEST`
    -- Exclude accounts with Bad_Flag_Cycle = 2 (excluded from analysis)
    WHERE SAFE_CAST(BAD_FLAG_CYCLE AS INT64) != 2
    GROUP BY 1
  )
  -- Exclude areas with no accounts
  WHERE total_accounts > 0
);
