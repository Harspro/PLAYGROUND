WITH
  input_index4_t0040_features_latest_load AS (
    SELECT
      MAX(input_index4_t0040_feature.REC_LOAD_TIMESTAMP)
        AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.INPUT_INDEX4_T0040_FEATURES`
        AS input_index4_t0040_feature
  )
SELECT
  input_index4_t0040_features.FEATURES,
  input_index4_t0040_features.REC_LOAD_TIMESTAMP,
  input_index4_t0040_features.JOB_ID
FROM
  `pcb-{env}-landing.domain_scoring.INPUT_INDEX4_T0040_FEATURES`
    AS input_index4_t0040_features
INNER JOIN
  input_index4_t0040_features_latest_load AS input_index4_t0040_features_ll
  ON
    input_index4_t0040_features.REC_LOAD_TIMESTAMP
    = input_index4_t0040_features_ll.LATEST_REC_LOAD_TIMESTAMP;