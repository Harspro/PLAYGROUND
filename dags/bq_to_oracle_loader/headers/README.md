# BQ to Oracle Header Files

This directory contains fixed header files for BigQuery exports that will be merged into a single file.

## Purpose

When exporting BigQuery tables to GCS with file merging enabled, header files stored here will be:
1. Uploaded to GCS
2. Composed at the beginning of the merged file
3. Used as the first row in the final merged CSV file

## How to Create Header Files

Manually fetch the column names from your BigQuery table and create a header file with the column names separated by your configured delimiter (default: `|`).

## File Format

- **Delimiter**: Use the same delimiter as configured in your `gcs_export.field_delimiter` (default: `|`)
- **Format**: Single line with column names separated by the delimiter
- **Line Ending**: MUST end with a newline character

## Example

For a table with columns: `ACCOUNT_ID`, `CUSTOMER_NAME`, `BALANCE`, `STATUS`

**File Content:**
```
ACCOUNT_ID|CUSTOMER_NAME|BALANCE|STATUS
```

## Configuration

**Header file is REQUIRED** when `merge_sharded_files.enabled` is `true` and `add_header` is `true`.

Specify the header file path in your DAG configuration:

```yaml
segment_file_names:
  CIF_CARD_CURR:
    output_filename: "pcb_cif_card_curr"
    file_extension: "{file_env}"
    gcs_path: "cif_exports"
    header_file: "bq_to_oracle_loader/headers/cif_card_curr_header.csv"  # Required
```

If the header file is not specified or not found, the task will fail with a clear error message.

## Benefits of Fixed Header Files

1. **Performance**: No BigQuery API call to fetch schema
2. **Control**: Specify exact column names/order
3. **Consistency**: Same header every time, even if BQ schema changes
4. **Simple**: Easy to maintain and understand

## Similar Pattern

This follows the same pattern as AML feeds (`dags/aml_processing/csv_headers_files/`).

