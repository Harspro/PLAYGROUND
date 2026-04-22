WITH output_trs_t0090_unnested_latest_load AS (
    SELECT
        MAX(output_trs_t0090_unnested.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
    FROM
        `pcb-{env}-landing.domain_scoring.OUTPUT_TRS_T0090_UNNESTED` AS output_trs_t0090_unnested
)
SELECT
    *
EXCEPT
    (LATEST_REC_LOAD_TIMESTAMP)
FROM
    `pcb-{env}-landing.domain_scoring.OUTPUT_TRS_T0090_UNNESTED` AS output_trs_t0090_unnested
    INNER JOIN output_trs_t0090_unnested_latest_load AS output_trs_t0090_unnested_ll ON output_trs_t0090_unnested.REC_LOAD_TIMESTAMP = output_trs_t0090_unnested_ll.LATEST_REC_LOAD_TIMESTAMP;