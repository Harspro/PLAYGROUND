-- Add the 5 new columns to child table BATCH_AUTHORIZATION_MC_TV
-- These columns are already present in the copybook,
-- but the existing table schema needs to be updated to include them.
-- Run this in BigQuery for each environment (dev, qa, prod)
-- Replace {env} with the appropriate environment name

ALTER TABLE `pcb-{env}-landing.domain_account_management.BATCH_AUTHORIZATION_MC_TV`
ADD COLUMN IF NOT EXISTS LG3_TKN_SERVICE_PROVIDER_ID STRING(5),
ADD COLUMN IF NOT EXISTS LG3_TKN_VERIF_METHOD STRING(5),
ADD COLUMN IF NOT EXISTS LG3_TKN_AUTH_FACTOR STRING(5),
ADD COLUMN IF NOT EXISTS LG3_PLUGIN_MSG_RESPONSE STRING(3),
ADD COLUMN IF NOT EXISTS LG3_PLUGIN_AUTH_RESPONSE STRING(2);
