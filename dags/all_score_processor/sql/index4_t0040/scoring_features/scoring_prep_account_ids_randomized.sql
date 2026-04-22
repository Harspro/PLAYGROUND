-- ============================================================================
-- Index4 T0040: Insert Account IDs with Random Mapping
-- ============================================================================
-- Purpose: Create persistent random ID mapping (0-9999) for each MAST_ACCOUNT_ID
--          Only includes "clean" accounts that pass business exclusion rules
--          Only inserts NEW accounts (idempotent - no duplicates)
--
-- Data Flow:
--   INPUT_INDEX4_T0040_FEATURES (our model input table)
--     + SCORING_PREP_PCMC_DATA_DEPOT (data depot with exclusion flags)
--     → THIS QUERY applies exclusions & generates random IDs
--     → pcb-{env}-landing.domain_scoring.ACCOUNT_IDS_RANDOMIZED
--
-- Business Purpose:
--   - **Anonymization**: Mask account IDs in test/dev environments
--   - **Stratified Sampling**: Evenly distribute accounts for testing
--   - **Reproducibility**: Same account always gets same random ID
--   - **Clean Universe**: Only includes accounts eligible for scoring
--
-- Exclusion Criteria (accounts are excluded if):
--   - Charge-off (CO_IND or CHGOFF_DT present)
--   - Bankruptcy (BK_IND present)
--   - Fraud (FR_IND present)
--   - Pending Insolvency (B_IND = 1)
--   - Credit Revoked (CRVK_IND or CRVK_DT present)
--   - Security Fraud (SEC_FRD_IND or SEC_FRD_DT present)
--   - Closed - Deceased (CLOSED_IND = 'DC')
--   - Voluntary Attrition (CLOSED_IND in specific codes)
--   - Involuntary Attrition (CLOSED_IND or CLOSED_DT present)
--   - Inactive (AM00_TRIAD_SCORE_ALIGNED = 13)
--   - Never Active (AM00_TRIAD_SCORE_ALIGNED = 14)
--
-- Random ID Generation:
--   - Uses FARM_FINGERPRINT hash function (BigQuery native)
--   - Deterministic: Same MAST_ACCOUNT_ID always produces same random ID
--   - Even Distribution: Hash function ensures uniform distribution 0-9999
--   - Fast: O(1) computation per account
--
-- Parameters:
--   {file_create_dt} - Date partition for INPUT_INDEX4_T0040_FEATURES (YYYY-MM-DD)
--   {report_date} - Business period for SCORING_PREP_PCMC_DATA_DEPOT (YYYYMM01)
--   {job_id} - Airflow job ID for audit tracking
--   {env} - Environment (dev, staging, prod)
-- ============================================================================

INSERT INTO `pcb-{env}-landing.domain_scoring.SCORING_PREP_ACCOUNT_IDS_RANDOMIZED`
(
  MAST_ACCOUNT_ID,
  GENERATED_RANDOM_ID,
  REC_LOAD_TIMESTAMP,
  JOB_ID
)
WITH
  -- ============================================================================
  -- STEP 0: Extract MAST_ACCOUNT_ID from features array (REPEATED RECORD)
  -- ============================================================================
  -- features is a REPEATED RECORD with feature_name and feature_value
  -- We need to unnest and filter for feature_name = 'MAST_ACCOUNT_ID'
  unnested_accounts AS (
    SELECT DISTINCT
      feature.feature_value AS MAST_ACCOUNT_ID
    FROM `pcb-{env}-curated.domain_scoring.INPUT_INDEX4_T0040_FEATURES_LATEST` dm
    CROSS JOIN UNNEST(dm.features) AS feature
    WHERE UPPER(feature.feature_name) = 'MAST_ACCOUNT_ID'
      AND feature.feature_value IS NOT NULL
  ),

  -- ============================================================================
  -- STEP 1: Get accounts from model input with exclusion flags from data depot
  -- ============================================================================
  accounts_with_exclusions AS (
    SELECT DISTINCT
      ua.MAST_ACCOUNT_ID,
      -- Apply exclusion logic from business rules
      CASE
        WHEN (dd.CO_IND IS NOT NULL) OR dd.CHGOFF_DT IS NOT NULL THEN 'Charge-off'
        WHEN dd.BK_IND IS NOT NULL THEN 'Charge-off Bankruptcy'
        WHEN dd.FR_IND IS NOT NULL THEN 'Charge-off Fraud'
        WHEN UPPER(dd.B_IND) = "1" THEN 'Pending Insolvency'
        WHEN dd.CRVK_IND IS NOT NULL OR dd.CRVK_DT IS NOT NULL THEN 'Credit Revoked'
        WHEN (dd.SEC_FRD_IND IS NOT NULL AND dd.SEC_FRD_IND <> '') OR dd.SEC_FRD_DT IS NOT NULL 
          THEN 'Security Fraud'
        WHEN (dd.FR_IND IS NOT NULL AND dd.AM00_STATF_FRAUD IS NOT NULL AND dd.AM00_STATF_FRAUD <> '') 
          THEN 'Fraud Flag'
        WHEN UPPER(dd.CLOSED_IND) = 'DC' THEN 'Closed Deceased'
        WHEN UPPER(dd.CLOSED_IND) IN ('BO', 'DU', 'IR', 'MS', 'OL', 'PG', 'US', 'V1', 'V2', 'V3', 'V4', 'V5', 'V6', 'V7', 'V8', 'V9')
          THEN 'Voluntary Attrition'
        WHEN (dd.CLOSED_IND IS NOT NULL AND dd.CLOSED_IND <> '') OR dd.CLOSED_DT IS NOT NULL 
          THEN 'Involuntary Attrition'
        WHEN dd.AM00_TRIAD_SCORE_ALIGNED = "13" THEN 'Inactive'
        WHEN dd.AM00_TRIAD_SCORE_ALIGNED = "14" THEN 'Never Active'
        ELSE ''
      END AS OBS_EXCLUSIONS
    FROM unnested_accounts ua
    INNER JOIN `pcb-{env}-curated.domain_scoring.SCORING_PREP_PCMC_DATA_DEPOT_LATEST` AS dd
      ON ua.MAST_ACCOUNT_ID = dd.MAST_ACCOUNT_ID
  ),

  -- ============================================================================
  -- STEP 2: Filter to only clean accounts (no exclusions)
  -- ============================================================================
  clean_accounts AS (
    SELECT DISTINCT MAST_ACCOUNT_ID
    FROM accounts_with_exclusions
    WHERE OBS_EXCLUSIONS = ''
  ),

  -- ============================================================================
  -- STEP 3: Generate deterministic random IDs (0-9999) using hash function
  -- ============================================================================
  accounts_with_random_ids AS (
    SELECT DISTINCT
      MAST_ACCOUNT_ID,
      -- FARM_FINGERPRINT returns INT64 hash, MOD ensures 0-9999 range
      CAST(MOD(ABS(FARM_FINGERPRINT(CAST(MAST_ACCOUNT_ID AS STRING))), 10000) AS STRING) AS GENERATED_RANDOM_ID
    FROM clean_accounts
  ),

  -- ============================================================================
  -- STEP 4: Identify NEW accounts (not already in target table)
  -- ============================================================================
  new_accounts_only AS (
    SELECT DISTINCT
      a.MAST_ACCOUNT_ID,
      a.GENERATED_RANDOM_ID
    FROM accounts_with_random_ids a
    LEFT JOIN `pcb-{env}-curated.domain_scoring.SCORING_PREP_ACCOUNT_IDS_RANDOMIZED_ALL` existing
      ON a.MAST_ACCOUNT_ID = existing.MAST_ACCOUNT_ID
    WHERE existing.MAST_ACCOUNT_ID IS NULL  -- Only accounts NOT already in table
  )

-- ============================================================================
-- STEP 5: Insert new mappings with audit fields
-- ============================================================================
SELECT DISTINCT
  MAST_ACCOUNT_ID,
  GENERATED_RANDOM_ID,
  CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
  '{dag_id}' AS JOB_ID
FROM new_accounts_only;

-- ============================================================================
-- IMPLEMENTATION NOTES
-- ============================================================================
--
-- 1. **Deterministic Randomization**: FARM_FINGERPRINT ensures same input always 
--    produces same output. This is critical for reproducibility across runs.
--
-- 2. **Even Distribution**: FARM_FINGERPRINT uses FarmHash algorithm which 
--    provides excellent distribution properties. All 10,000 buckets should have 
--    roughly equal number of accounts (±5%).
--
-- 3. **Idempotent Inserts**: LEFT JOIN anti-pattern ensures we only insert 
--    accounts that don't already exist. This allows safe re-runs without 
--    duplicates or conflicts.
--
-- 4. **Immutability**: Once an account gets a random ID, it NEVER changes. 
--    This is enforced by only inserting new accounts, never updating existing.
--
-- 5. **Exclusion Logic**: Replicates business rules from sample SQL. Accounts 
--    with any exclusion flag are not included in the mapping table.
--
-- 6. **Performance**: 
--    - Clean accounts query: Fast (filters on indexed fields)
--    - Hash generation: O(1) per account
--    - Deduplication LEFT JOIN: Fast (MAST_ACCOUNT_ID clustering)
--    - Expected runtime: < 5 minutes for millions of accounts
--
-- 7. **Monitoring**: Track counts of:
--    - Total accounts in INPUT_INDEX4_T0040_FEATURES
--    - Accounts excluded (by exclusion type)
--    - Clean accounts eligible for mapping
--    - New accounts added to mapping table
--    - Total mappings in table (should grow monotonically)
--
-- ============================================================================
-- VALIDATION QUERIES
-- ============================================================================
--
-- After insertion, validate with:
--
-- -- Check total mappings
-- SELECT COUNT(*) AS total_mappings
-- FROM `pcb-{env}-landing.domain_scoring.ACCOUNT_IDS_RANDOMIZED`;
--
-- -- Verify no duplicates
-- SELECT MAST_ACCOUNT_ID, COUNT(*) AS dup_count
-- FROM `pcb-{env}-landing.domain_scoring.ACCOUNT_IDS_RANDOMIZED`
-- GROUP BY MAST_ACCOUNT_ID
-- HAVING COUNT(*) > 1;
-- -- Expected: 0 rows
--
-- -- Check random ID distribution (should be roughly even)
-- SELECT 
--   FLOOR(CAST(GENERATED_RANDOM_ID AS INT64) / 100) * 100 AS id_bucket,
--   COUNT(*) AS accounts_in_bucket
-- FROM `pcb-{env}-landing.domain_scoring.ACCOUNT_IDS_RANDOMIZED`
-- GROUP BY 1
-- ORDER BY 1;
-- -- Expected: Each bucket has similar count (variance < 20%)
--
-- -- Check random ID range
-- SELECT 
--   MIN(CAST(GENERATED_RANDOM_ID AS INT64)) AS min_id,
--   MAX(CAST(GENERATED_RANDOM_ID AS INT64)) AS max_id,
--   COUNT(DISTINCT GENERATED_RANDOM_ID) AS unique_ids
-- FROM `pcb-{env}-landing.domain_scoring.ACCOUNT_IDS_RANDOMIZED`;
-- -- Expected: min=0, max=9999, unique_ids close to 10000
--
-- -- Compare new inserts vs existing
-- SELECT 
--   DATE(REC_LOAD_TIMESTAMP) AS load_date,
--   JOB_ID,
--   COUNT(*) AS new_accounts_added
-- FROM `pcb-{env}-landing.domain_scoring.ACCOUNT_IDS_RANDOMIZED`
-- GROUP BY 1, 2
-- ORDER BY 1 DESC;
--
-- ============================================================================
