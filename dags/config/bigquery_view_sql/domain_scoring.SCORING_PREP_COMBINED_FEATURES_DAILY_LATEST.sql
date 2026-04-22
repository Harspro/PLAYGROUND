WITH
  combined_features_daily_latest_load AS (
    SELECT
      MAX(combined_features_daily.REC_LOAD_TIMESTAMP)
        AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.SCORING_PREP_COMBINED_FEATURES_DAILY`
        AS combined_features_daily
  )
SELECT * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_COMBINED_FEATURES_DAILY`
    AS combined_features_daily
INNER JOIN
  combined_features_daily_latest_load AS combined_features_daily_ll
  ON
    combined_features_daily.REC_LOAD_TIMESTAMP
    = combined_features_daily_ll.LATEST_REC_LOAD_TIMESTAMP;