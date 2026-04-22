-- Index4 T0040: Insert postal code risk ranking reference data into Landing Table
-- This query pulls historical postal code bad rates used to calculate area risk category
--
-- NOTE: The source table contains 8 fields. Only 3 are actively used in area_risk_lookup
-- calculation (CIFP_ZIP_CODE, MAST_ACCOUNT_ID_t0, Bad_Flag_Cycle), but we materialize
-- all 8 fields as this is a small reference table and having complete data may be useful
-- for future analysis or model iterations.

INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_POSTAL_CODE_RI_V4_RISK_RANKING`
(
  MAST_ACCOUNT_ID_T0,
  BAD_FLAG_CYCLE,
  CIFP_ZIP_CODE,
  EXCLUSION_REASON_T0,
  PERF_EXCLUSIONS_24,
  AH01_BALANCE_CURRENT_t0,
  BCN_T0,
  PRIME_IND,
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
SELECT DISTINCT
  -- All source fields cast to STRING
  CAST(MAST_ACCOUNT_ID_t0 AS STRING) AS MAST_ACCOUNT_ID_T0,
  CAST(Bad_Flag_Cycle AS STRING) AS BAD_FLAG_CYCLE,
  CAST(CIFP_ZIP_CODE AS STRING) AS CIFP_ZIP_CODE,
  CAST(Exclusion_Reason_t0 AS STRING) AS EXCLUSION_REASON_T0,
  CAST(PERF_EXCLUSIONS_24 AS STRING) AS PERF_EXCLUSIONS_24,
  CAST(AH01_BALANCE_CURRENT_t0 AS STRING) AS AH01_BALANCE_CURRENT_T0,
  CAST(bcn_t0 AS STRING) AS BCN_T0,
  CAST(prime_ind AS STRING) AS PRIME_IND,

  -- Audit fields (generated at insert time)
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM `{creditrisk-scoring-project}.{creditrisk-scoring-dataset}.postal_code_ri_v4_risk_ranking_042023`;