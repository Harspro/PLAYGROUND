-- File: sql/table_comparison/create_mismatch_report.sql
-- Delete any existing data for the current run date to prevent duplicates
DELETE FROM `{mismatch_report_table_fq}`
WHERE run_date = CURRENT_DATE();

-- Insert new comparison results with run date
INSERT INTO `{mismatch_report_table_fq}`
(
    run_date,
    {pk_columns_for_clustering},
    record_comparison_status,
    differing_column_details
)
WITH source_filtered AS (
    SELECT
        {pk_columns_source_aliased} -- s.id AS pk_id
        {compare_columns_source_aliased} -- s.colA AS s_colA
        {original_columns_source_aliased} -- s.colA AS s_orig_colA (for reporting)
    FROM
        `{source_table_fq}` AS s
    {source_filter_clause} -- e.g., WHERE s.load_date = '2023-01-01'
),
target_filtered AS (
    SELECT
        {pk_columns_target_aliased} -- t.id AS pk_id
        {compare_columns_target_aliased} -- t.colA AS t_colA
        {original_columns_target_aliased} -- t.colA AS t_orig_colA (for reporting)
    FROM
        `{target_table_fq}` AS t
    {target_filter_clause}
)
SELECT
    CURRENT_DATE() AS run_date,
    {pk_select_coalesce}, -- COALESCE(s.pk_id, t.pk_id) AS id
    CASE
        WHEN s.{first_pk_column_source_aliased} IS NULL THEN 'MISSING_IN_SOURCE' -- Record exists in target only
        WHEN t.{first_pk_column_target_aliased} IS NULL THEN 'MISSING_IN_TARGET' -- Record exists in source only
        WHEN {mismatch_predicate_sql} THEN 'MISMATCH' -- Dynamically built: (NOT(s.s_colA IS NOT DISTINCT FROM t.t_colA)) OR ...
        ELSE 'MATCH'
    END AS record_comparison_status,
    ARRAY_CONCAT(
        CASE WHEN s.{first_pk_column_source_aliased} IS NULL THEN [STRUCT('record_status' AS column_name, CAST(NULL AS STRING) AS source_original_value, 'MISSING_IN_SOURCE' AS target_original_value, CAST(NULL AS STRING) AS source_transformed_value, 'MISSING_IN_SOURCE' AS target_transformed_value)] ELSE [] END,
        CASE WHEN t.{first_pk_column_target_aliased} IS NULL THEN [STRUCT('record_status' AS column_name, 'MISSING_IN_TARGET' AS source_original_value, CAST(NULL AS STRING) AS target_original_value, 'MISSING_IN_TARGET' AS source_transformed_value, CAST(NULL AS STRING) AS target_transformed_value)] ELSE [] END,
        {diff_details_structs_sql} -- Dynamically built: IF(NOT(s.s_colA IS NOT DISTINCT FROM t.t_colA), STRUCT(...), NULL), ...
    ) AS differing_column_details -- Will contain NULLs for non-differing columns, can be filtered later or use ARRAY_REMOVE_ALL in BQ
    -- You can also select individual source/target columns if preferred over the array:
    -- comparison_columns_select_sql -- e.g., s.s_colA as source_colA, t.t_colA as target_colA
FROM source_filtered s
FULL OUTER JOIN target_filtered t ON {join_on_clause_aliased} -- s.pk_id = t.pk_id AND ...
WHERE
    s.{first_pk_column_source_aliased} IS NULL OR -- Missing in source
    t.{first_pk_column_target_aliased} IS NULL OR -- Missing in target
    ({mismatch_predicate_sql}); -- Mismatched values