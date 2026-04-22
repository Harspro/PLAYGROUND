WITH source_pks AS (
    SELECT DISTINCT {pk_columns_list_sql}
    FROM `{source_table_fq}` AS s
    {source_filter_clause} -- WHERE s.load_date = ...
),
target_pks AS (
    SELECT DISTINCT {pk_columns_list_sql}
    FROM `{target_table_fq}` AS t
    {target_filter_clause} -- WHERE t.load_date = ...
)
SELECT COUNT(*) AS missing_in_source_count
FROM target_pks t
LEFT JOIN source_pks s ON {join_on_clause_sql} -- s.id = t.id AND ...
WHERE s.{first_pk_column} IS NULL; -- PK from source side is NULL