WITH source_filtered AS (
    SELECT {pk_columns_list_sql}, {compare_columns_list_sql}
    FROM `{source_table_fq}` AS s
    {source_filter_clause} -- WHERE s.load_date = ...
),
target_filtered AS (
    SELECT {pk_columns_list_sql}, {compare_columns_list_sql}
    FROM `{target_table_fq}` AS t
    {target_filter_clause} -- WHERE t.load_date = ...
)
SELECT COUNT(*) AS mismatched_record_count
FROM source_filtered s
FULL OUTER JOIN target_filtered t ON {join_on_clause_sql} -- s.id = t.id AND ...
WHERE s.{first_pk_column} IS NOT NULL -- Ensure both records exist for a mismatch
  AND t.{first_pk_column} IS NOT NULL
  AND ({mismatch_predicate_sql}); -- (NOT(s.colA IS NOT DISTINCT FROM t.colA)) OR ...