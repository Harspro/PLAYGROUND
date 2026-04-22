WITH
  output_index4_t0040_latest_load AS (
    SELECT
      MAX(output_index4_t0040.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.OUTPUT_INDEX4_T0040`
        AS output_index4_t0040
  )
SELECT
  output_index4_t0040.OUTPUTS,
  output_index4_t0040.EXECUTION_ID,
  output_index4_t0040.REC_LOAD_TIMESTAMP,
  output_index4_t0040.JOB_ID
FROM
  `pcb-{env}-landing.domain_scoring.OUTPUT_INDEX4_T0040` AS output_index4_t0040
INNER JOIN
  output_index4_t0040_latest_load AS output_index4_t0040_ll
  ON
    output_index4_t0040.REC_LOAD_TIMESTAMP
    = output_index4_t0040_ll.LATEST_REC_LOAD_TIMESTAMP;