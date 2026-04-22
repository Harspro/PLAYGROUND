WITH
  segmented_input_daily_latest_load AS (
    SELECT
      MAX(segmented_input_daily.REC_LOAD_TIMESTAMP)
        AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.SCORING_PREP_SEGMENTED_INPUT_DAILY`
        AS segmented_input_daily
  )
SELECT * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_SEGMENTED_INPUT_DAILY`
    AS segmented_input_daily
INNER JOIN
  segmented_input_daily_latest_load AS segmented_input_daily_ll
  ON
    segmented_input_daily.REC_LOAD_TIMESTAMP
    = segmented_input_daily_ll.LATEST_REC_LOAD_TIMESTAMP;