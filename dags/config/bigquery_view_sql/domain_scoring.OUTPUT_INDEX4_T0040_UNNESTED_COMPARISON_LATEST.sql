WITH output_index4_t0040_unnested_comparison_latest_load AS (
    SELECT
        MAX(output_index4_t0040_unnested_comparison.REC_LOAD_TIMESTAMP) AS LATEST_REC_LOAD_TIMESTAMP
    FROM
        `pcb-{env}-landing.domain_scoring.OUTPUT_INDEX4_T0040_UNNESTED_COMPARISON` AS output_index4_t0040_unnested_comparison
)
SELECT
    *
EXCEPT
    (LATEST_REC_LOAD_TIMESTAMP)
FROM
    `pcb-{env}-landing.domain_scoring.OUTPUT_INDEX4_T0040_UNNESTED_COMPARISON` AS output_index4_t0040_unnested_comparison
    INNER JOIN output_index4_t0040_unnested_comparison_latest_load AS output_index4_t0040_unnested_comparison_ll ON output_index4_t0040_unnested_comparison.REC_LOAD_TIMESTAMP = output_index4_t0040_unnested_comparison_ll.LATEST_REC_LOAD_TIMESTAMP;