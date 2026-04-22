CREATE OR REPLACE VIEW `{view_name}` AS
SELECT *
FROM `{mismatch_report_table_name}`
WHERE 1=1
  -- Add any additional filtering logic here if needed
  -- For example: AND mismatch_type IS NOT NULL
; 