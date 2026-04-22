WITH
  scoring_prep_trans_summary_daily_latest_load AS (
    SELECT
      MAX(scoring_prep_trans_summary_daily.REC_LOAD_TIMESTAMP)
        AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.SCORING_PREP_TRANS_SUMMARY_DAILY` AS scoring_prep_trans_summary_daily
  )
SELECT * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_TRANS_SUMMARY_DAILY` AS scoring_prep_trans_summary_daily
INNER JOIN
  scoring_prep_trans_summary_daily_latest_load AS scoring_prep_trans_summary_daily_ll
  ON
    scoring_prep_trans_summary_daily.REC_LOAD_TIMESTAMP = scoring_prep_trans_summary_daily_ll.LATEST_REC_LOAD_TIMESTAMP;
