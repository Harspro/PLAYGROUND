-- File: sql/table_comparison/get_schema_mismatches.sql
WITH source_schema AS (
  SELECT column_name, data_type, ordinal_position
  FROM `{source_project}.{source_dataset}.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = '{source_table_name}'
),
target_schema AS (
  SELECT column_name, data_type, ordinal_position
  FROM `{target_project}.{target_dataset}.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = '{target_table_name}'
)
SELECT
  COALESCE(s.column_name, t.column_name) as column_name_coalesced,
  s.column_name AS source_column_name,
  s.data_type AS source_data_type,
  s.ordinal_position AS source_ordinal_position,
  t.column_name AS target_column_name,
  t.data_type AS target_data_type,
  t.ordinal_position AS target_ordinal_position,
  CASE
    WHEN s.column_name IS NULL THEN 'COLUMN_MISSING_IN_SOURCE'
    WHEN t.column_name IS NULL THEN 'COLUMN_MISSING_IN_TARGET'
    WHEN s.data_type != t.data_type THEN 'DATA_TYPE_MISMATCH'
    -- Optional: Add ordinal position check if column order matters strictly
    -- WHEN s.ordinal_position != t.ordinal_position THEN 'ORDINAL_POSITION_MISMATCH'
    ELSE 'NO_MISMATCH_DETECTED_HERE' -- Should not be hit due to WHERE clause
  END AS schema_discrepancy_type
FROM source_schema s
FULL OUTER JOIN target_schema t ON s.column_name = t.column_name
WHERE s.column_name IS NULL
   OR t.column_name IS NULL
   OR s.data_type != t.data_type
   -- Optional: OR s.ordinal_position != t.ordinal_position
ORDER BY COALESCE(s.ordinal_position, t.ordinal_position, 999), column_name_coalesced;