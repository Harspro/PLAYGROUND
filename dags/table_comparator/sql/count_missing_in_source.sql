-- File: sql/table_comparison/count_missing_in_source.sql
SELECT COUNT(*) AS missing_in_source_count
FROM `{mismatch_report_table_fq}`
WHERE record_comparison_status = 'MISSING_IN_SOURCE'
  AND run_date = CURRENT_DATE();