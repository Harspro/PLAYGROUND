WITH
  output_trs_t0090_latest_load AS (
    SELECT
      MAX(output_trs_t0090.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
    FROM
      `pcb-{env}-landing.domain_scoring.OUTPUT_TRS_T0090` AS output_trs_t0090
  )
SELECT
  output_trs_t0090.OUTPUTS,
  output_trs_t0090.EXECUTION_ID,
  output_trs_t0090.REC_LOAD_TIMESTAMP,
  output_trs_t0090.JOB_ID
FROM
  `pcb-{env}-landing.domain_scoring.OUTPUT_TRS_T0090` AS output_trs_t0090
INNER JOIN
  output_trs_t0090_latest_load AS output_trs_t0090_ll
  ON
    output_trs_t0090.REC_LOAD_TIMESTAMP
    = output_trs_t0090_ll.LATEST_REC_LOAD_TIMESTAMP;