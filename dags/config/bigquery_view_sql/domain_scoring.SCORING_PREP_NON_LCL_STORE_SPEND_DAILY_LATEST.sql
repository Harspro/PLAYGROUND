  WITH
  scoring_prep_non_lcl_store_spend_daily_latest_load AS (
  SELECT
    MAX(scoring_prep_non_lcl_store_spend_daily.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-landing.domain_scoring.SCORING_PREP_NON_LCL_STORE_SPEND_DAILY` AS scoring_prep_non_lcl_store_spend_daily )
SELECT
  * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_NON_LCL_STORE_SPEND_DAILY` AS scoring_prep_non_lcl_store_spend_daily
INNER JOIN
  scoring_prep_non_lcl_store_spend_daily_latest_load AS scoring_prep_non_lcl_store_spend_daily_ll
ON
  scoring_prep_non_lcl_store_spend_daily.REC_LOAD_TIMESTAMP = scoring_prep_non_lcl_store_spend_daily_ll.LATEST_REC_LOAD_TIMESTAMP;