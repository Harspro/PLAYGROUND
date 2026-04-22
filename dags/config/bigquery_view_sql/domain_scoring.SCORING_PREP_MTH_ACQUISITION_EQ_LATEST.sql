WITH
  scoring_prep_mth_acquisition_eq_latest_load AS (
  SELECT
    MAX(scoring_prep_mth_acquisition_eq.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
  FROM
    `pcb-{env}-landing.domain_scoring.SCORING_PREP_MTH_ACQUISITION_EQ` AS scoring_prep_mth_acquisition_eq )
SELECT * EXCEPT(LATEST_REC_LOAD_TIMESTAMP)
FROM
  `pcb-{env}-landing.domain_scoring.SCORING_PREP_MTH_ACQUISITION_EQ` AS scoring_prep_mth_acquisition_eq
INNER JOIN
  scoring_prep_mth_acquisition_eq_latest_load AS scoring_prep_mth_acquisition_eq_ll
ON
  scoring_prep_mth_acquisition_eq.REC_LOAD_TIMESTAMP = scoring_prep_mth_acquisition_eq_ll.LATEST_REC_LOAD_TIMESTAMP;