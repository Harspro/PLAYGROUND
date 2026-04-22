WITH
  scoring_prep_wk_acquisition_latest_load AS (
  SELECT
    MAX(scoring_prep_wk_acquisition.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_ACQUISITION` AS scoring_prep_wk_acquisition )
SELECT
  * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_WK_ACQUISITION` AS scoring_prep_wk_acquisition
INNER JOIN
  scoring_prep_wk_acquisition_latest_load AS scoring_prep_wk_acquisition_ll
ON
  scoring_prep_wk_acquisition.REC_LOAD_TIMESTAMP = scoring_prep_wk_acquisition_ll.LATEST_REC_LOAD_TIMESTAMP;