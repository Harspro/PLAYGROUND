-- File: sql/table_comparison/create_mismatch_report_table_schema.sql
-- Create the mismatch report table with proper schema for appending data
CREATE TABLE IF NOT EXISTS `{mismatch_report_table_fq}`
(
    run_date DATE NOT NULL,
    {pk_columns_schema},
    record_comparison_status STRING NOT NULL,
    differing_column_details ARRAY<STRUCT<
        column_name STRING,
        source_original_value STRING,
        target_original_value STRING,
        source_transformed_value STRING,
        target_transformed_value STRING
    >>
)
PARTITION BY run_date
CLUSTER BY {pk_columns_for_clustering}; 