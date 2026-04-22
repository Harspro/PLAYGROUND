-- File: sql/table_comparison/count_mismatched_records.sql
SELECT COUNT(*)
FROM `{mismatch_report_table_fq}`
WHERE record_comparison_status = 'MISMATCH'
  AND run_date = CURRENT_DATE();