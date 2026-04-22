# BigQuery Table Comparison DAG

A configurable Airflow DAG for comparing two BigQuery tables and identifying differences in data and schema.

## Project Structure

```
dags/table_comparator/
├── __init__.py                           # Package initialization
├── table_comparator_dag_base.py          # Base DAG class
├── table_comparator_dag_loader.py        # Main DAG implementation
├── table_comparator_dag_launcher.py      # DAG launcher
├── utils.py                              # Utility functions
├── comparator_config.yaml                # Configuration template
├── README.md                             # This documentation
└── sql/                                  # SQL templates
    ├── create_mismatch_report.sql
    ├── count_mismatched_records.sql
    ├── count_missing_in_source.sql
    ├── count_missing_in_target.sql
    ├── count_mismatched_records_direct.sql
    ├── count_missing_in_source_direct.sql
    ├── count_missing_in_target_direct.sql
    └── get_schema_mismatches.sql
```

## Features

- **Comprehensive Comparison**: Compares data records, identifies missing records, and detects schema mismatches
- **Configurable**: YAML-based configuration for easy customization
- **Flexible Filtering**: SQL-like filtering conditions for subset comparisons
- **Column Exclusion**: Exclude specific columns from comparison (e.g., audit fields, system-generated columns)
- **Expression Support**: Apply transformations to columns before comparison (e.g., TRIM, UPPER, COALESCE)
- **Detailed Reporting**: Generates comprehensive mismatch reports with original and transformed values
- **Schema Validation**: Detects column and data type differences
- **Performance Monitoring**: Built-in timing and statistics
- **Robust Error Handling**: Comprehensive validation and error messages
- **Run Date Tracking**: Adds run date column to track when each comparison was executed
- **Append Mode**: Preserves historical comparison results instead of overwriting


## Run Date Tracking and Append Mode

The table comparator now supports run date tracking and append mode to preserve historical comparison results.

### Key Benefits
1. **Historical Tracking**: Each comparison run is tagged with a run date for easy identification
2. **Data Preservation**: Comparison results are appended while preventing duplicates for the same day
3. **Audit Trail**: Complete audit trail of all comparison runs
4. **Run-Specific Counts**: Counts are filtered by run date to show results for the current execution

### Implementation Details

#### 1. Run Date Column
- **Automatic addition**: A `run_date` column is automatically added to all mismatch reports
- **Current date**: Uses `CURRENT_DATE()` to tag each comparison run
- **Partitioning**: The mismatch report table is partitioned by `run_date` for efficient querying

#### 2. Append Mode with Duplicate Prevention
- **Table creation**: Uses `CREATE TABLE IF NOT EXISTS` to create the table schema once
- **Duplicate prevention**: Deletes any existing data for the current run date before inserting new results
- **Data insertion**: Uses `INSERT INTO` to add new results while preserving historical data from previous days
- **Schema preservation**: Table schema is created separately and preserved across runs

#### 3. Run-Specific Counts
- **Filtered queries**: All count queries now filter by `run_date = CURRENT_DATE()`
- **Current run focus**: Counts show results only for the current execution
- **Historical context**: Optional historical analysis provides broader context



### Configuration

#### Table Schema
The mismatch report table now has this structure:
```sql
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
PARTITION BY DATE(run_date)
CLUSTER BY {pk_columns_for_clustering};
```

### Usage Examples

#### Basic Configuration
```yaml
cif_curr_comparator:
  source_table_name: pcb-{env}-landing.domain_account_management.IDL_CIF_CARD_CURR
  target_table_name: pcb-{env}-landing.domain_account_management.CIF_CARD_CURR
  primary_key_columns:
    - CIFP_ACCOUNT_ID6
    - FILE_CREATE_DT
  mismatch_report_table_name: pcb-{env}-landing.domain_account_management.CIF_CARD_CURR_COMPARE_RESULTS
```

#### Query by Run Date
```sql
-- Get all comparison results for a specific date
SELECT * FROM `{mismatch_report_table_fq}`
WHERE run_date = '2024-01-15';
```

## Exclude Columns Feature

The `exclude_columns` feature allows users to exclude specific columns from comparison while maintaining all existing functionality.

### Key Benefits
1. **Flexibility**: Users can easily exclude system-generated or audit columns
2. **Data Quality**: Focus comparison on business-relevant data
3. **Performance**: Reduce comparison overhead by excluding irrelevant columns
4. **Maintainability**: Clean separation of concerns
5. **Backward Compatibility**: Existing configurations continue to work

### Implementation Details

#### 1. Configuration Structure
- **Simple list format**: `exclude_columns` accepts a list of column names to exclude
- **Case-insensitive matching**: Column names are normalized to lowercase for comparison
- **Optional parameter**: Users can specify `exclude_columns` or omit it entirely

#### 2. Exclusion Logic Priority
- **Exclusion takes precedence**: When both `comparison_columns` and `exclude_columns` are specified, exclusion wins
- **Example**: 
  - `comparison_columns: ["col1", "col2", "col3"]`
  - `exclude_columns: ["col2"]`
  - **Result**: Only `["col1", "col3"]` will be compared

#### 3. Primary Key Protection
- **Primary keys cannot be excluded**: System automatically prevents exclusion of primary key columns
- **Warning logged**: If user attempts to exclude PK columns, a warning is logged and the exclusion is ignored
- **Safety first**: Ensures data integrity by maintaining PK comparison

#### 4. Schema Mismatch Handling
- **Independent validation**: Schema mismatches are still reported even for excluded columns
- **Complete schema visibility**: Users get full visibility into table structure differences

#### 5. Expression Interaction
- **No expressions applied**: Excluded columns are completely ignored in all processing
- **Clean separation**: Excluded columns don't participate in any transformation logic

#### 6. Validation and Error Handling
- **Warning for non-existent columns**: If excluded columns don't exist, warnings are logged but processing continues
- **Graceful degradation**: System continues to work even with invalid exclusion lists

#### 7. Default Behavior
- **Backward compatible**: Existing configurations work exactly as before
- **All common non-PK columns**: When neither `comparison_columns` nor `exclude_columns` is specified, all common non-PK columns are compared

#### 8. Logging and Reporting
- **Basic logging**: Shows which columns were excluded and the count
- **Clear reporting**: Excluded columns are listed in the final summary report

### Usage Examples

#### Basic Exclusion
```yaml
exclude_columns:
  - "audit_timestamp"
  - "last_modified_by"
  - "version_number"
```

#### Exclusion with Comparison Columns
```yaml
comparison_columns:
  - "amount"
  - "status"
  - "description"
  - "audit_timestamp"
exclude_columns:
  - "audit_timestamp"
# Result: Only "amount", "status", "description" will be compared
```

#### Case-Insensitive Matching
```yaml
exclude_columns:
  - "AUDIT_TIMESTAMP"
  - "Last_Modified_By"
# Will exclude "audit_timestamp", "last_modified_by" regardless of case

### Testing Results
- **All tests passing**: 4/4 tests successful
- **Functionality verified**: 
  - Basic exclusion works correctly
  - Case-insensitive matching works
  - Primary key protection works
  - No exclusions scenario works

### Future Enhancements
- **Pattern matching**: Support for wildcard exclusions (e.g., `audit_*`)
- **Column groups**: Predefined exclusion groups for common scenarios
- **Dynamic exclusions**: Runtime determination of columns to exclude
- **Exclusion reasons**: Track why columns were excluded for audit purposes

### Files Modified for Exclude Columns Feature

#### 1. `dags/table_comparator/utils.py`
- **Modified function**: `determine_comparison_columns()`
- **Added parameter**: `exclude_columns_list: Optional[List[str]] = None`
- **Added logic**: 
  - Case-insensitive column matching
  - Primary key protection
  - Exclusion precedence over comparison columns
  - Comprehensive logging

#### 2. `dags/table_comparator/table_comparator_dag_loader.py`
- **Modified function**: `generate_comparison_queries()`
- **Added parameter extraction**: `exclude_columns_list = kwargs.get('exclude_columns')`
- **Updated function call**: Pass `exclude_columns_list` to `determine_comparison_columns()`
- **Enhanced reporting**: Added excluded columns to summary report

#### 3. `dags/table_comparator/comparator_config.yaml`
- **Added example**: `exclude_columns` configuration with sample audit columns
- **Demonstrates usage**: Shows how to exclude system-generated columns

#### 4. `tests/dags/table_comparator/test_exclude_columns_simple.py`
- **Comprehensive test suite**: Tests all major functionality
- **Mocked dependencies**: Avoids import issues in test environment
- **Test coverage**: 
  - Basic exclusion functionality
  - Case-insensitive matching
  - Primary key protection
  - No exclusions scenario

## Quick Start

### 1. Configuration

Create a configuration file `comparator_config.yaml`:

```yaml
# BigQuery Table Comparison Configuration
source_table_name: "your-project.dataset.source_table"
target_table_name: "your-project.dataset.target_table"

# Primary key columns (required)
primary_key_columns:
  - "id"
  - "event_date"

# Flexible filtering with SQL-like conditions (replaces date filtering)
# Apply same filters to both source and target tables
filters:
  - "load_date = CURRENT_DATE()"
  - "status = 'ACTIVE'"
  # - "amount > 1000"
  # - "created_date >= '2024-01-01'"

# Optional: Specific columns to compare
comparison_columns:
  - "status"
  - "amount"
  - "description"

# Optional: Columns to exclude from comparison (exclusion takes precedence over comparison_columns)
exclude_columns:
  - "audit_timestamp"
  - "last_modified_by"
  - "version_number"

# Type-based global expressions applied to comparison columns
# These expressions are applied based on column data types
global_expressions_by_type:
  string_types:
    - "TRIM(column)"
    - "COALESCE(column, '')"  # Treat NULL as empty string
  numeric_types:
    - "COALESCE(column, 0)"   # Treat NULL as 0
    - "ROUND(column, 2)"      # Round to 2 decimal places

# Column-specific expressions (override global for specific columns)
# These expressions completely replace global expressions for the specified column
column_expressions:
  amount:
    expressions:
      - "ROUND(column, 2)"
    description: "Compare amounts with precision handling"
  
  phone_number:
    expressions:
      - "REGEXP_REPLACE(column, '[^0-9]', '')"
    description: "Compare only numeric digits in phone numbers"

# Optional: Detailed mismatch report table
mismatch_report_table_name: "your-project.dataset.comparison_mismatch_details"
```

### 2. Trigger the DAG

Trigger the DAG with your configuration:

```bash
# Using Airflow CLI
airflow dags trigger bigquery_table_comparison_from_config \
  --conf '{
    "source_table_name": "project.dataset.source",
    "target_table_name": "project.dataset.target",
    "primary_key_columns": ["id"],
    "comparison_columns": ["status", "amount"]
  }'
```

## Configuration Options

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `source_table_name` | string | Fully qualified source table name (project.dataset.table) |
| `target_table_name` | string | Fully qualified target table name (project.dataset.table) |
| `primary_key_columns` | list | List of column names that uniquely identify records |

### Optional Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `gcp_conn_id` | string | GCP connection ID (default: "google_cloud_default") |
| `gcp_project_id` | string | GCP project ID (default: from connection) |
| `filters` | list | List of SQL-like filter conditions to apply to both tables |
| `comparison_columns` | list | Specific columns to compare (default: all common non-PK columns) |
| `exclude_columns` | list | Columns to exclude from comparison (exclusion takes precedence over comparison_columns) |
| `global_expressions_by_type` | dict | Type-based global expressions |
| `column_expressions` | dict | Column-specific expressions (override global for specific columns) |
| `mismatch_report_table_name` | string | Table to store detailed mismatch report |

## Usage Examples

### Basic Comparison

```yaml
source_table_name: "my-project.dataset.users_source"
target_table_name: "my-project.dataset.users_target"
primary_key_columns:
  - "user_id"
```

### Incremental Comparison

```yaml
source_table_name: "my-project.dataset.transactions_source"
target_table_name: "my-project.dataset.transactions_target"
primary_key_columns:
  - "transaction_id"
record_load_date_column: "transaction_date"
specific_load_dates:
  - "2024-01-15"
  - "2024-01-16"
```

### Specific Column Comparison

```yaml
source_table_name: "my-project.dataset.products_source"
target_table_name: "my-project.dataset.products_target"
primary_key_columns:
  - "product_id"
comparison_columns:
  - "price"
  - "status"
  - "category"
```

### Excluding Columns from Comparison

```yaml
source_table_name: "my-project.dataset.users_source"
target_table_name: "my-project.dataset.users_target"
primary_key_columns:
  - "user_id"
# Exclude system-generated or audit columns from comparison
exclude_columns:
  - "audit_timestamp"
  - "last_modified_by"
  - "version_number"
  - "created_date"
```

### Excluding Columns with Comparison Columns

```yaml
source_table_name: "my-project.dataset.transactions_source"
target_table_name: "my-project.dataset.transactions_target"
primary_key_columns:
  - "transaction_id"
comparison_columns:
  - "amount"
  - "status"
  - "description"
  - "audit_timestamp"
  - "last_modified_by"
# Exclude audit columns (exclusion takes precedence)
exclude_columns:
  - "audit_timestamp"
  - "last_modified_by"
# Result: Only "amount", "status", "description" will be compared
```

### Filtering Data for Subset Comparison

The `filters` feature allows you to compare only a subset of data from both tables using SQL-like conditions. This is useful for incremental comparisons or when you want to focus on specific data segments.

#### Basic Filtering

```yaml
source_table_name: "my-project.dataset.transactions_source"
target_table_name: "my-project.dataset.transactions_target"
primary_key_columns:
  - "transaction_id"
# Compare only today's data
filters:
  - "load_date = CURRENT_DATE()"
```

#### Multiple Filter Conditions

```yaml
source_table_name: "my-project.dataset.users_source"
target_table_name: "my-project.dataset.users_target"
primary_key_columns:
  - "user_id"
# Compare only active users from specific regions
filters:
  - "status = 'ACTIVE'"
  - "region IN ('US', 'CA')"
  - "created_date >= '2024-01-01'"
```

#### Complex Filter Conditions

```yaml
source_table_name: "my-project.dataset.orders_source"
target_table_name: "my-project.dataset.orders_target"
primary_key_columns:
  - "order_id"
# Compare high-value orders from recent dates
filters:
  - "total_amount > 1000"
  - "order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)"
  - "status IN ('COMPLETED', 'PROCESSING')"
```

#### Filtering with Other Features

```yaml
source_table_name: "my-project.dataset.products_source"
target_table_name: "my-project.dataset.products_target"
primary_key_columns:
  - "product_id"
# Filter active products, exclude audit columns, apply expressions
filters:
  - "is_active = true"
  - "category IN ('ELECTRONICS', 'BOOKS')"
exclude_columns:
  - "audit_timestamp"
  - "last_modified_by"
global_expressions_by_type:
  string_types:
    - "TRIM(column)"
    - "COALESCE(column, '')"
```

### With Expression Support

```yaml
source_table_name: "my-project.dataset.products_source"
target_table_name: "my-project.dataset.products_target"
primary_key_columns:
  - "product_id"
comparison_columns:
  - "price"
  - "status"
  - "description"
  - "created_date"
  - "is_active"

# Type-based global expressions
global_expressions_by_type:
  string_types:
    - "TRIM(column)"
    - "COALESCE(column, '')"
  numeric_types:
    - "COALESCE(column, 0)"
    - "ROUND(column, 2)"
  date_types:
    - "DATE(column)"
  boolean_types:
    - "COALESCE(column, false)"

column_expressions:
  price:
    expressions:
      - "ROUND(column, 2)"
    description: "Compare prices with 2 decimal precision"
  status:
    expressions:
      - "UPPER(column)"
    description: "Case-insensitive status comparison"
```

### With Detailed Reporting

```yaml
source_table_name: "my-project.dataset.orders_source"
target_table_name: "my-project.dataset.orders_target"
primary_key_columns:
  - "order_id"
comparison_columns:
  - "total_amount"
  - "status"
mismatch_report_table_name: "my-project.dataset.order_mismatches"
```

## Output

The DAG provides comprehensive output including:

### Summary Report
```
--- BigQuery Table Comparison Results ---
Source Table: project.dataset.source
Target Table: project.dataset.target
Primary Keys: ['id']
Excluded Columns: ['audit_timestamp', 'last_modified_by']
Applied Type-Based Global Expressions:
  - string_types: TRIM(column), COALESCE(column, '')
  - numeric_types: COALESCE(column, 0), ROUND(column, 2)
Column-Specific Expressions:
  - amount: ROUND(column, 2) (Compare amounts with precision handling)
  - phone_number: REGEXP_REPLACE(column, '[^0-9]', '') (Compare only numeric digits in phone numbers)

Mismatched Records Count: 15
Records Missing in Source (present in target only): 3
Records Missing in Target (present in source only): 7

--- Schema Mismatches ---
Column: status | Source Type: STRING | Target Type: INT64 | Discrepancy: DATA_TYPE_MISMATCH

--- Performance Summary ---
Total execution time: 45.23 seconds
Total records with issues: 25
Mismatch rate: 60.00%
```

### Detailed Mismatch Report (if enabled)

When `mismatch_report_table_name` is provided, a detailed table is created with:
- Primary key values
- Record comparison status (MISMATCH, MISSING_IN_SOURCE, MISSING_IN_TARGET)
- Detailed column differences with source and target values (both original and transformed)
- Expression metadata for each column

## Troubleshooting

### Common Issues

#### 1. Table Not Found
```
Error: One or both tables not found
```
**Solution**: Verify table names and ensure they exist in the specified project/dataset.

#### 2. Invalid Table Name Format
```
Table name must be in format 'project.dataset.table'
```
**Solution**: Ensure table names follow the format `project.dataset.table`.

#### 3. Invalid Date Format
```
Invalid date format(s): ['2024/01/15']. Dates must be in YYYY-MM-DD format.
```
**Solution**: Use YYYY-MM-DD format for dates in `specific_load_dates`.

#### 4. Column Not Found
```
Comparison column 'column_name' not found in both source and target tables.
```
**Solution**: Verify column names exist in both tables.

#### 5. No Comparison Columns Found
```
Warning: No comparison columns found. Only primary key existence will be checked.
```
**Solution**: Check if tables have common columns beyond primary keys.

#### 6. Primary Key Exclusion Warning
```
Warning: Primary key columns cannot be excluded: ['id']. These will be included in comparison.
```
**Solution**: This is expected behavior. Primary key columns are automatically protected from exclusion to maintain data integrity.

#### 7. Excluded Columns Not Found
```
Warning: Excluded columns not found in tables: ['nonexistent_column']
```
**Solution**: This warning is informational. The system continues processing even if excluded columns don't exist in the tables.

### Performance Optimization

1. **Use Filtering**: For large tables, use `filters` to compare only relevant data subsets
2. **Limit Comparison Columns**: Specify only necessary columns in `comparison_columns`
3. **Exclude Irrelevant Columns**: Use `exclude_columns` to skip system-generated or audit columns
4. **Use Clustering**: Ensure tables are clustered on primary key columns
5. **Monitor Execution Time**: Check performance summary in logs

### Expression Usage

### Type-Based Global Expressions
Apply different expressions based on column data types:
```yaml
global_expressions_by_type:
  string_types:
    - "TRIM(column)"           # Remove leading/trailing whitespace
    - "COALESCE(column, '')"   # Treat NULL as empty string
  numeric_types:
    - "COALESCE(column, 0)"    # Treat NULL as 0
    - "ROUND(column, 2)"       # Round to 2 decimal places
  date_types:
    - "DATE(column)"           # Extract date part only
  boolean_types:
    - "COALESCE(column, false)" # Treat NULL as false
```

### Column-Specific Expressions
Column-specific expressions completely override global expressions for that column:
```yaml
column_expressions:
  amount:
    expressions:
      - "ROUND(column, 2)"   # Round to 2 decimal places
    description: "Compare amounts with precision handling"
  
  phone_number:
    expressions:
      - "REGEXP_REPLACE(column, '[^0-9]', '')"  # Remove non-digits
    description: "Compare only numeric digits"
```

### Type-Based Global Expressions

The `global_expressions_by_type` feature allows you to apply different expressions based on column data types. This is the recommended approach for handling mixed data types.

**Supported Data Type Categories:**

**String Types:**
- STRING, VARCHAR, CHAR, TEXT, LONGTEXT, MEDIUMTEXT, TINYTEXT

**Numeric Types:**
- INT64, INTEGER, INT, BIGINT, SMALLINT, TINYINT
- NUMERIC, DECIMAL, BIGNUMERIC, BIGDECIMAL
- FLOAT64, FLOAT, DOUBLE, REAL

**Date Types:**
- DATE, DATETIME, TIMESTAMP

**Boolean Types:**
- BOOL, BOOLEAN

**Example:**
```yaml
global_expressions_by_type:
  string_types:
    - "TRIM(column)"
    - "COALESCE(column, '')"
  numeric_types:
    - "COALESCE(column, 0)"
    - "ROUND(column, 2)"
  date_types:
    - "DATE(column)"
  boolean_types:
    - "COALESCE(column, false)"

# This will apply:
# - String expressions to: description (STRING), status (VARCHAR)
# - Numeric expressions to: amount (INT64), price (DECIMAL)
# - Date expressions to: created_date (DATE), updated_at (TIMESTAMP)
# - Boolean expressions to: is_active (BOOL)
```



### Common Use Cases

#### Type-Based NULL Handling
```yaml
global_expressions_by_type:
  string_types:
    - "COALESCE(column, '')"   # NULL as empty string
  numeric_types:
    - "COALESCE(column, 0)"    # NULL as 0
  boolean_types:
    - "COALESCE(column, false)" # NULL as false
```

#### Case-Insensitive String Comparison
```yaml
global_expressions_by_type:
  string_types:
    - "TRIM(column)"
    - "UPPER(column)"
```

#### Numeric Precision Handling
```yaml
global_expressions_by_type:
  numeric_types:
    - "ROUND(column, 2)"       # Round to 2 decimals
    - "COALESCE(column, 0)"    # NULL as 0
```



#### Column-Specific Precision Handling
```yaml
column_expressions:
  amount:
    expressions:
      - "CAST(ROUND(column, 2) AS DECIMAL(10,2))"
```

#### Column-Specific Data Cleaning
```yaml
column_expressions:
  description:
    expressions:
      - "TRIM(column)"
      - "REGEXP_REPLACE(column, '\\s+', ' ')"  # Normalize whitespace
```

## Best Practices

1. **Primary Key Selection**: Choose columns that uniquely identify records
2. **Data Filtering**: Use `filters` for incremental comparisons and subset analysis
3. **Column Selection**: Only compare relevant columns to improve performance
4. **Expression Validation**: Test expressions on sample data before production use
5. **Error Handling**: Monitor logs for warnings and errors
6. **Testing**: Test with small datasets before running on large tables

## Monitoring

The DAG provides comprehensive monitoring through:

- **Execution Time**: Total and per-task timing information
- **Record Counts**: Detailed statistics on mismatches and missing records
- **Schema Validation**: Automatic detection of schema differences
- **Error Logging**: Detailed error messages and warnings

## Architecture

The DAG is organized into several components:

### 1. Base Class (`table_comparator_dag_base.py`)
- Abstract base class defining the DAG structure
- Contains common DAG configuration and task definitions
- Provides the foundation for different DAG implementations

### 2. Main Implementation (`table_comparator_dag_loader.py`)
- Concrete implementation of the table comparison logic
- Contains the `generate_comparison_queries()` and `report_results_callable()` methods
- Handles query generation, execution, and result reporting

### 3. DAG Launcher (`table_comparator_dag_launcher.py`)
- Simple launcher that creates DAG instances from configuration
- Uses the loader to generate DAGs dynamically

### 4. Utilities (`utils.py`)
- Contains all helper functions for validation, SQL generation, and BigQuery operations
- Modular design for easy testing and maintenance
- Includes functions for:
  - Input validation
  - Schema retrieval
  - SQL alias generation
  - Query generation
  - BigQuery job execution
  - Column exclusion logic

## Dependencies

- Apache Airflow 2.0+
- Google Cloud BigQuery
- `google-cloud-bigquery` Python package
- `apache-airflow-providers-google` package

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review Airflow logs for detailed error messages
3. Verify configuration parameters
4. Test with smaller datasets first 