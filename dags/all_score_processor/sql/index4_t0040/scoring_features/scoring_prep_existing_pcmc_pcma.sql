-- Index4 T0040: Insert EXISTING_PCMC_PCMA into Landing Table
-- This query loads PC Financial to PC Optimum wallet mapping from the business layer.
--
-- Data Flow:
--   lt-* tables (raw)
--     → (business creates) pcb-prod-creditrisk.SHARE_CUST_RISK_ACQ.existing_pcmc_pcma
--     → THIS QUERY loads from business table
--     → pcb-{env}-landing.domain_scoring.SCORING_PREP_EXISTING_PCMC_PCMA (34 fields)
--
-- Business Purpose:
--   Critical bridge table linking wallet_id (shopping behavior) to MAST_ACCOUNT_ID (credit accounts).
--   Without this mapping, PRE_APP_SCORE and PCO_SCORE cannot be joined to credit data.
--
-- Parameters:
--   {report_year} - Report year (e.g., 2024)
--   {report_month} - Report month (e.g., 04)
--   {job_id} - Airflow job ID for audit tracking
--   {env} - Environment (dev, staging, prod)
INSERT INTO
    `pcb-{env}-landing.domain_scoring.SCORING_PREP_EXISTING_PCMC_PCMA` (
        REPORT_DATE,
        WALLET_ID,
        CONSUMER_ID,
        PCF_CUST_ID,
        PCO_REGISTERED,
        PCO_REGISTERED_DATE,
        CONSUMER_DATE_CREATED,
        CONSUMER_EFFECTIVE_TS,
        CONSUMER_TYPE,
        CONSUMER_STATE,
        CONSUMER_STATUS,
        CUSTOMER_SINCE_DATE,
        WALLET_DATE_CREATED,
        WALLET_EFFECTIVE_TS,
        CONSUMER_WALLET_TYPE,
        CONSUMER_WALLET_STATE,
        CONSUMER_WALLET_STATUS,
        HOUSEHOLD_WALLET_ID,
        HOUSEHOLD_DATE_CREATED,
        BUSINESS_EFFECTIVE_TS,
        HOUSEHOLD_WALLET_STATE,
        HOUSEHOLD_WALLET_STATUS,
        PRODUCT_MIX,
        CUSTOMER_TYPE,
        PRODUCT_TYPE,
        MAIN_PRODUCT_TYPE,
        CIFP_EMAIL_ADDRESS_LINE1,
        CIFP_EMAIL_ADDRESS_LINE2,
        MAST_ACCOUNT_ID,
        PHX_ACCOUNT_ID,
        DT_RANK,
        REC_LOAD_TIMESTAMP,
        JOB_ID
    )
SELECT DISTINCT
    -- Metadata fields
    CONCAT('{report_year}', LPAD('{report_month}', 2, '0'), '01') AS REPORT_DATE,
    -- Data fields (all cast to STRING)
    CAST(wallet_id AS STRING) AS wallet_id,
    CAST(consumer_id AS STRING) AS consumer_id,
    CAST(PCF_CUST_ID AS STRING) AS PCF_CUST_ID,
    CAST(PCO_Registered AS STRING) AS PCO_Registered,
    CAST(PCO_Registered_Date AS STRING) AS PCO_Registered_Date,
    CAST(consumer_date_created AS STRING) AS consumer_date_created,
    CAST(consumer_effective_ts AS STRING) AS consumer_effective_ts,
    CAST(consumer_type AS STRING) AS consumer_type,
    CAST(consumer_state AS STRING) AS consumer_state,
    CAST(consumer_status AS STRING) AS consumer_status,
    CAST(customer_since_date AS STRING) AS customer_since_date,
    CAST(wallet_date_created AS STRING) AS wallet_date_created,
    CAST(wallet_effective_ts AS STRING) AS wallet_effective_ts,
    CAST(consumer_wallet_type AS STRING) AS consumer_wallet_type,
    CAST(consumer_wallet_state AS STRING) AS consumer_wallet_state,
    CAST(consumer_wallet_status AS STRING) AS consumer_wallet_status,
    CAST(household_wallet_id AS STRING) AS household_wallet_id,
    CAST(household_date_created AS STRING) AS household_date_created,
    CAST(business_effective_ts AS STRING) AS business_effective_ts,
    CAST(household_wallet_state AS STRING) AS household_wallet_state,
    CAST(household_wallet_status AS STRING) AS household_wallet_status,
    CAST(PRODUCT_MIX AS STRING) AS PRODUCT_MIX,
    CAST(CUSTOMER_TYPE AS STRING) AS CUSTOMER_TYPE,
    CAST(PRODUCT_TYPE AS STRING) AS PRODUCT_TYPE,
    CAST(MAIN_PRODUCT_TYPE AS STRING) AS MAIN_PRODUCT_TYPE,
    CAST(CIFP_EMAIL_ADDRESS_LINE1 AS STRING) AS CIFP_EMAIL_ADDRESS_LINE1,
    CAST(CIFP_EMAIL_ADDRESS_LINE2 AS STRING) AS CIFP_EMAIL_ADDRESS_LINE2,
    CAST(MAST_ACCOUNT_ID AS STRING) AS MAST_ACCOUNT_ID,
    CAST(PHX_ACCOUNT_ID AS STRING) AS PHX_ACCOUNT_ID,
    CAST(dt_rank AS STRING) AS dt_rank,
    -- Audit fields
    CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
    '{dag_id}' AS JOB_ID
FROM
    `{creditrisk-project}.{creditrisk-dataset}.existing_pcmc_pcma`;