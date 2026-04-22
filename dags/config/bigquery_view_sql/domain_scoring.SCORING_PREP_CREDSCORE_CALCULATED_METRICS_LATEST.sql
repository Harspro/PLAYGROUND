  WITH
  scoring_prep_credscore_calculated_metrics_latest_load AS (
  SELECT
    MAX(scoring_prep_credscore_calculated_metrics.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-landing.domain_scoring.SCORING_PREP_CREDSCORE_CALCULATED_METRICS` AS scoring_prep_credscore_calculated_metrics )
SELECT
  * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_CREDSCORE_CALCULATED_METRICS` AS scoring_prep_credscore_calculated_metrics
INNER JOIN
  scoring_prep_credscore_calculated_metrics_latest_load AS scoring_prep_credscore_calculated_metrics_ll
ON
  scoring_prep_credscore_calculated_metrics.REC_LOAD_TIMESTAMP = scoring_prep_credscore_calculated_metrics_ll.LATEST_REC_LOAD_TIMESTAMP;