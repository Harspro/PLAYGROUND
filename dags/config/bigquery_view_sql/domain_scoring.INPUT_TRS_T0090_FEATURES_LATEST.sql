WITH
  input_trs_t0090_features_latest_load AS (
    SELECT
      MAX(input_trs_t0090_feature.REC_LOAD_TIMESTAMP)
        AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.INPUT_TRS_T0090_FEATURES`
        AS input_trs_t0090_feature
  )
SELECT
  input_trs_t0090_features.FEATURES,
  input_trs_t0090_features.REC_LOAD_TIMESTAMP,
  input_trs_t0090_features.JOB_ID
FROM
  `pcb-{env}-landing.domain_scoring.INPUT_TRS_T0090_FEATURES`
    AS input_trs_t0090_features
INNER JOIN
  input_trs_t0090_features_latest_load AS input_trs_t0090_features_ll
  ON
    input_trs_t0090_features.REC_LOAD_TIMESTAMP
    = input_trs_t0090_features_ll.LATEST_REC_LOAD_TIMESTAMP;