"""
Utility functions for BigQuery Table Comparison DAG.

This module contains all helper functions used by the BigQuery table comparison DAG,
including SQL file reading, BigQuery job execution, validation functions, and
query generation logic.
"""

from google.cloud.exceptions import NotFound
from google.cloud import bigquery
import time
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
import logging
logger = logging.getLogger(__name__)


# --- Configuration for SQL File Location ---
DAG_FOLDER = Path(__file__).resolve().parent
SQL_FILES_SUBDIR = "sql"  # Relative to DAG file


# --- Helper Functions for Validation ---
def validate_date_format(date_str: str) -> bool:
    """Validate if date string is in YYYY-MM-DD format."""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False


def validate_table_name_format(table_name: str) -> bool:
    """Validate if table name follows project.dataset.table format."""
    return table_name.count('.') == 2


def validate_expressions(expressions: List[str], column_name: str) -> None:
    """
    Validate expression syntax and safety.

    Args:
        expressions: List of expressions to validate
        column_name: Name of the column for error reporting

    Raises:
        ValueError: If unsafe or invalid expression detected
    """
    for expr in expressions:
        # Basic SQL injection prevention
        unsafe_keywords = [
            'DROP',
            'DELETE',
            'INSERT',
            'UPDATE',
            'CREATE',
            'ALTER']
        if any(keyword in expr.upper() for keyword in unsafe_keywords):
            raise ValueError(
                f"Unsafe expression detected for column {column_name}: {expr}")

        # Check for 'column' placeholder
        if 'column' not in expr:
            logger.warning(
                f"Expression for {column_name} doesn't use 'column' placeholder: {expr}")


def build_column_expression(column_name: str, expressions: List[str]) -> str:
    """
    Build final SQL expression by chaining multiple expressions.

    Args:
        column_name: Original column name
        expressions: List of expressions to apply in sequence

    Returns:
        Final SQL expression with all transformations applied
    """
    if not expressions:
        return f"`{column_name}`"

    result = f"`{column_name}`"
    for expr in expressions:
        # Replace 'column' placeholder with current result
        result = expr.replace('column', result)

    return result


def build_column_expression_with_alias(
        column_name: str,
        expressions: List[str],
        table_alias: str) -> str:
    """
    Build final SQL expression by chaining multiple expressions with table alias.

    Args:
        column_name: Original column name
        expressions: List of expressions to apply in sequence
        table_alias: Table alias (e.g., 's' for source, 't' for target)

    Returns:
        Final SQL expression with table alias and all transformations applied
    """
    if not expressions:
        return f"{table_alias}.`{column_name}`"

    # Start with the column reference with table alias
    result = f"{table_alias}.`{column_name}`"
    for expr in expressions:
        # Replace 'column' placeholder with current result
        result = expr.replace('column', result)

    return result


def get_data_type_category(data_type: str) -> Optional[str]:
    """
    Get the category of a BigQuery data type.

    Args:
        data_type: BigQuery data type string

    Returns:
        Category name ('string_types', 'numeric_types', 'date_types', 'boolean_types') or None if not categorized
    """
    data_type_upper = data_type.upper()

    # String types
    string_types = {
        'STRING',
        'VARCHAR',
        'CHAR',
        'TEXT',
        'LONGTEXT',
        'MEDIUMTEXT',
        'TINYTEXT'}

    # Numeric types
    numeric_types = {
        'INT64', 'INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'TINYINT',
        'NUMERIC', 'DECIMAL', 'BIGNUMERIC', 'BIGDECIMAL',
        'FLOAT64', 'FLOAT', 'DOUBLE', 'REAL'
    }

    # Date types
    date_types = {
        'DATE', 'DATETIME', 'TIMESTAMP'
    }

    # Boolean types
    boolean_types = {
        'BOOL', 'BOOLEAN'
    }

    if data_type_upper in string_types:
        return 'string_types'
    elif data_type_upper in numeric_types:
        return 'numeric_types'
    elif data_type_upper in date_types:
        return 'date_types'
    elif data_type_upper in boolean_types:
        return 'boolean_types'
    else:
        return None


def get_column_expressions(
    column_name: str,
    column_expressions: Optional[Dict[str, Dict[str, Any]]] = None,
    source_cols_schema: Optional[Dict[str, Any]] = None,
    target_cols_schema: Optional[Dict[str, Any]] = None,
    global_expressions_by_type: Optional[Dict[str, List[str]]] = None
) -> List[str]:
    """
    Get the expressions to apply for a specific column.

    Args:
        column_name: Name of the column
        column_expressions: Column-specific expressions
        source_cols_schema: Source table schema for data type checking
        target_cols_schema: Target table schema for data type checking
        global_expressions_by_type: Type-based global expressions

    Returns:
        List of expressions to apply (column-specific override global)
    """
    # Check if column has specific expressions (these always override global)
    if column_expressions and column_name in column_expressions:
        return column_expressions[column_name].get('expressions', [])

    # Check for type-based global expressions
    if global_expressions_by_type and source_cols_schema and target_cols_schema:
        if (column_name in source_cols_schema and column_name in target_cols_schema):
            source_type = source_cols_schema[column_name].field_type
            target_type = target_cols_schema[column_name].field_type

            # Get data type categories for both source and target
            source_category = get_data_type_category(source_type)
            target_category = get_data_type_category(target_type)

            # Only apply if both columns have the same category and it's
            # defined in global_expressions_by_type
            if source_category and target_category and source_category == target_category:
                if source_category in global_expressions_by_type:
                    logger.info(
                        f"Applying {source_category} expressions to column '{column_name}' ({source_type})")
                    return global_expressions_by_type[source_category]
                else:
                    logger.info(
                        f"No expressions defined for {source_category} type column '{column_name}'")
                    return []
            else:
                logger.info(
                    f"Skipping type-based expressions for column '{column_name}' (source: {source_type}, target: {target_type})")
                return []
        else:
            logger.warning(
                f"Column '{column_name}' not found in both schemas, skipping type-based expressions")
            return []

    return []


# --- Helper Function to Read SQL Files ---
def read_sql_file(sql_file_name: str) -> str:
    """Reads an SQL file from the predefined SQL directory and returns its content."""
    full_path = DAG_FOLDER / SQL_FILES_SUBDIR / sql_file_name
    try:
        with open(full_path, 'r') as f:
            return f.read()
    except FileNotFoundError:
        logger.error(f"SQL file not found at {full_path}")
        raise
    except Exception as e:
        logger.error(f"Error reading SQL file {sql_file_name}: {e}")
        raise


# --- Helper Function to Log BigQuery Job Statistics ---
def log_bigquery_job_statistics(
    query_job: bigquery.QueryJob,
    job_start_time: float,
    fetch_results: bool = False,
    results: Optional[bigquery.table.RowIterator] = None
) -> None:
    """
    Log comprehensive BigQuery job statistics.

    Args:
        query_job: The completed BigQuery query job
        job_start_time: Start time of the job execution
        fetch_results: Whether this was a SELECT query with results
        results: Query results (only for SELECT queries)
    """
    job_elapsed_time = time.time() - job_start_time

    logger.info("=== BigQuery Job Statistics ===")
    logger.info(f"Job ID: {query_job.job_id}")
    logger.info(f"Statement Type: {query_job.statement_type}")
    logger.info(f"Execution Time: {job_elapsed_time:.2f} seconds")
    logger.info(f"Total Bytes Processed: {query_job.total_bytes_processed:,}")
    logger.info(f"Total Bytes Billed: {query_job.total_bytes_billed:,}")
    logger.info(f"Slot Milliseconds: {query_job.slot_millis:,}")
    logger.info(f"Cache Hit: {query_job.cache_hit}")
    logger.info(f"Destination Table: {query_job.destination}")
    logger.info(f"Job State: {query_job.state}")

    # Log SELECT-specific statistics
    if fetch_results and results:
        logger.info(f"Total Rows: {results.total_rows}")

    # # Log DDL/DML specific information
    # if hasattr(query_job, 'num_dml_affected_rows'):
    #     logger.info(f"DML Affected Rows: {query_job.num_dml_affected_rows}")

    # Log performance metrics
    if query_job.total_bytes_processed > 0:
        bytes_per_second = query_job.total_bytes_processed / job_elapsed_time
        logger.info(f"Processing Rate: {bytes_per_second:,.0f} bytes/second")

    if query_job.slot_millis > 0:
        slots_used = query_job.slot_millis / (job_elapsed_time * 1000)
        logger.info(f"Average Slots Used: {slots_used:.2f}")

    logger.info("=== End Job Statistics ===")


# --- Helper Function to Execute BigQuery Jobs ---
def execute_bigquery_job(
        query_string: str,
        fetch_results: bool = False,
        job_config_kwargs: Optional[Dict[str, Any]] = None,
        **kwargs  # Airflow context, not used directly here but good practice
) -> Optional[List[List[Any]]]:
    """
    Execute a BigQuery job and optionally fetch results.

    Args:
        query_string: SQL query to execute
        fetch_results: Whether to fetch and return results
        job_config_kwargs: Optional BigQuery job configuration
        **kwargs: Additional Airflow context (unused)

    Returns:
        List of results if fetch_results=True, None otherwise
    """
    job_start_time = time.time()
    if not query_string or query_string.strip() == "":
        logger.info("Query string is empty. Skipping BigQuery execution.")
        if fetch_results:
            return []
        return None

    try:
        client = bigquery.Client()
        job_config_args = job_config_kwargs or {}
        job_config = bigquery.QueryJobConfig(**job_config_args)

        logger.info(
            f"Executing BigQuery job \n {query_string}")
        query_job = client.query(query_string, job_config=job_config)

        # Log job creation
        logger.info(f"BigQuery job created with ID: {query_job.job_id}")
        logger.info(f"Job location: {query_job.location}")
        logger.info(f"Job created at: {query_job.created}")

        if fetch_results:
            results = query_job.result()

            # Log comprehensive statistics for SELECT queries
            log_bigquery_job_statistics(
                query_job,
                job_start_time,
                fetch_results=True,
                results=results)

            return [list(row.values()) for row in results]
        else:
            results = query_job.result()  # Wait for DDL/DML to complete

            # Log comprehensive statistics for DDL/DML queries
            log_bigquery_job_statistics(
                query_job, job_start_time, fetch_results=False)

            if query_job.errors:
                logger.error(
                    f"BigQuery job {query_job.job_id} failed with errors: {query_job.errors}")
                raise Exception(
                    f"BigQuery job {query_job.job_id} failed: {query_job.errors}")
            return None
    except Exception as e:
        job_elapsed_time = time.time() - job_start_time
        logger.error(
            f"BigQuery job execution failed after {job_elapsed_time:.2f} seconds: {e}")
        raise


# --- Helper Function to Validate Input Parameters ---
def validate_input_parameters(
    source_table_fq: str,
    target_table_fq: str,
    pk_columns_list: List[str]
) -> None:
    """
    Validate all input parameters for the table comparison.

    Args:
        source_table_fq: Fully qualified source table name
        target_table_fq: Fully qualified target table name
        pk_columns_list: List of primary key columns

    Raises:
        ValueError: If any validation fails
    """
    if not pk_columns_list:
        raise ValueError("Primary key columns list cannot be empty.")

    # Validate table name formats
    if not validate_table_name_format(source_table_fq):
        raise ValueError(
            f"Source table name '{source_table_fq}' must be in format 'project.dataset.table'")
    if not validate_table_name_format(target_table_fq):
        raise ValueError(
            f"Target table name '{target_table_fq}' must be in format 'project.dataset.table'")


# --- Helper Function to Get Table Schemas ---
def get_table_schemas(
    source_table_fq: str,
    target_table_fq: str,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Get schemas for both source and target tables.

    Args:
        source_table_fq: Fully qualified source table name
        target_table_fq: Fully qualified target table name
    Returns:
        Tuple of (source_schema, target_schema)

    Raises:
        NotFound: If one or both tables don't exist
    """
    client = bigquery.Client()

    try:
        source_table_obj = client.get_table(source_table_fq)
        target_table_obj = client.get_table(target_table_fq)
    except NotFound as e:
        logger.info(f"Error: One or both tables not found: {e}")
        raise

    source_cols_schema = {
        field.name: field for field in source_table_obj.schema}
    target_cols_schema = {
        field.name: field for field in target_table_obj.schema}

    return source_cols_schema, target_cols_schema


# --- Helper Function to Determine Comparison Columns ---
def determine_comparison_columns(
    user_compare_cols_list: Optional[List[str]],
    source_cols_schema: Dict[str, Any],
    target_cols_schema: Dict[str, Any],
    pk_columns_list: List[str],
    exclude_columns_list: Optional[List[str]] = None
) -> List[str]:
    """
    Determine which columns to compare based on user input or automatic detection.

    Args:
        user_compare_cols_list: User-provided comparison columns
        source_cols_schema: Source table schema
        target_cols_schema: Target table schema
        pk_columns_list: List of primary key columns
        exclude_columns_list: List of columns to exclude from comparison

    Returns:
        List of columns to compare
    """
    final_compare_cols_list = []

    # Normalize exclude columns to lowercase for case-insensitive matching
    normalized_exclude_cols = []
    if exclude_columns_list:
        normalized_exclude_cols = [col.strip().lower()
                                   for col in exclude_columns_list]
        logger.info(
            f"Excluding columns from comparison: {exclude_columns_list}")

        # Check for primary key columns in exclude list
        pk_in_exclude = [
            pk for pk in pk_columns_list if pk.lower() in normalized_exclude_cols]
        if pk_in_exclude:
            logger.warning(
                f"Primary key columns cannot be excluded: {pk_in_exclude}. These will be included in comparison.")
            # Remove PK columns from exclude list
            normalized_exclude_cols = [
                col for col in normalized_exclude_cols if col not in [
                    pk.lower() for pk in pk_columns_list]]

    if user_compare_cols_list:  # If user provided comparison columns
        final_compare_cols_list = [col.strip()
                                   for col in user_compare_cols_list]

        # Apply exclusions (exclusion takes precedence)
        if normalized_exclude_cols:
            original_count = len(final_compare_cols_list)
            final_compare_cols_list = [
                col for col in final_compare_cols_list
                if col.lower() not in normalized_exclude_cols
            ]
            excluded_count = original_count - len(final_compare_cols_list)
            if excluded_count > 0:
                logger.info(
                    f"Excluded {excluded_count} columns from user-specified comparison columns")

        for col in final_compare_cols_list:  # Validation
            if col not in source_cols_schema or col not in target_cols_schema:
                raise ValueError(
                    f"Comparison column '{col}' not found in both source and target tables.")
            if col in pk_columns_list:  # Warning if a PK is also in compare list
                logger.info(
                    f"Warning: Comparison column '{col}' is also a primary key.")
    else:  # Default to common non-PK columns
        common_cols_names = set(
            source_cols_schema.keys()) & set(
            target_cols_schema.keys())
        final_compare_cols_list = [
            col for col in common_cols_names if col not in pk_columns_list]

        # Apply exclusions to default columns
        if normalized_exclude_cols:
            original_count = len(final_compare_cols_list)
            final_compare_cols_list = [
                col for col in final_compare_cols_list
                if col.lower() not in normalized_exclude_cols
            ]
            excluded_count = original_count - len(final_compare_cols_list)
            if excluded_count > 0:
                logger.info(
                    f"Excluded {excluded_count} columns from default comparison columns")

        if not final_compare_cols_list:
            logger.info(
                "Warning: No comparison columns found. Only primary key existence will be checked.")
            logger.info(
                f"Available columns in source: {list(source_cols_schema.keys())}")
            logger.info(
                f"Available columns in target: {list(target_cols_schema.keys())}")
            logger.info(f"Primary key columns: {pk_columns_list}")
            if exclude_columns_list:
                logger.info(f"Excluded columns: {exclude_columns_list}")

    return final_compare_cols_list


# --- Helper Function to Generate SQL Aliases ---
def generate_sql_aliases(
    pk_columns_list: List[str],
    final_compare_cols_list: List[str],
    column_expressions: Optional[Dict[str, Dict[str, Any]]] = None,
    source_cols_schema: Optional[Dict[str, Any]] = None,
    target_cols_schema: Optional[Dict[str, Any]] = None,
    global_expressions_by_type: Optional[Dict[str, List[str]]] = None
) -> Dict[str, Any]:
    """
    Generate SQL aliases for primary keys and comparison columns with expression support.

    Args:
        pk_columns_list: List of primary key columns
        final_compare_cols_list: List of comparison columns
        column_expressions: Column-specific expressions
        source_cols_schema: Source table schema for data type checking
        target_cols_schema: Target table schema for data type checking
        global_expressions_by_type: Type-based global expressions

    Returns:
        Dictionary containing all generated SQL aliases
    """
    # Primary key aliases
    pk_cols_s_aliased = [
        f"s.`{col}` AS s_pk_{i}" for i, col in enumerate(pk_columns_list)]
    pk_cols_t_aliased = [
        f"t.`{col}` AS t_pk_{i}" for i, col in enumerate(pk_columns_list)]

    pk_select_coalesce = ", ".join([
        f"COALESCE(s.s_pk_{i}, t.t_pk_{i}) AS `{pk_columns_list[i]}`"
        for i in range(len(pk_columns_list))
    ])

    join_on_aliased = " AND ".join([
        f"s.s_pk_{i} = t.t_pk_{i}" for i in range(len(pk_columns_list))
    ])

    first_pk_s_alias = "s_pk_0"  # From aliased PKs for source table
    first_pk_t_alias = "t_pk_0"  # From aliased PKs for target table

    # Comparison column aliases with expressions
    comp_cols_s_aliased = []
    comp_cols_t_aliased = []
    original_cols_s_aliased = []  # For storing original values
    original_cols_t_aliased = []  # For storing original values

    for col in final_compare_cols_list:
        # Get expressions for this column
        expressions = get_column_expressions(
            col,
            column_expressions,
            source_cols_schema,
            target_cols_schema,
            global_expressions_by_type
        )

        # Validate expressions
        if expressions:
            validate_expressions(expressions, col)

        # Build the final expressions with proper table aliases
        s_alias = f"s_cmp_{col.replace('-', '_')}"
        t_alias = f"t_cmp_{col.replace('-', '_')}"

        comp_cols_s_aliased.append(
            f"{build_column_expression_with_alias(col, expressions, 's')} AS {s_alias}")
        comp_cols_t_aliased.append(
            f"{build_column_expression_with_alias(col, expressions, 't')} AS {t_alias}")

        # Store original values for reporting
        original_cols_s_aliased.append(
            f"s.`{col}` AS s_orig_{col.replace('-', '_')}")
        original_cols_t_aliased.append(
            f"t.`{col}` AS t_orig_{col.replace('-', '_')}")

    # Mismatch conditions and diff details
    mismatch_conditions_aliased = []
    diff_details_structs_aliased = []

    for col in final_compare_cols_list:
        s_alias = f"s_cmp_{col.replace('-', '_')}"
        t_alias = f"t_cmp_{col.replace('-', '_')}"
        s_orig_alias = f"s_orig_{col.replace('-', '_')}"
        t_orig_alias = f"t_orig_{col.replace('-', '_')}"

        mismatch_conditions_aliased.append(
            f"(NOT(s.{s_alias} IS NOT DISTINCT FROM t.{t_alias}))")

        # Include both original and transformed values in diff details
        diff_details_structs_aliased.append(
            f"IF(NOT(s.{s_alias} IS NOT DISTINCT FROM t.{t_alias}), "
            f"STRUCT('`{col}`' AS column_name, "
            f"CAST(s.{s_orig_alias} AS STRING) AS source_original_value, "
            f"CAST(t.{t_orig_alias} AS STRING) AS target_original_value, "
            f"CAST(s.{s_alias} AS STRING) AS source_transformed_value, "
            f"CAST(t.{t_alias} AS STRING) AS target_transformed_value), NULL)"
        )

    mismatch_predicate_sql = " OR ".join(
        mismatch_conditions_aliased) if mismatch_conditions_aliased else "FALSE"
    diff_details_structs_sql = (
        f"ARRAY(SELECT x FROM UNNEST([{', '.join(diff_details_structs_aliased if diff_details_structs_aliased else ['NULL'])}]) AS x WHERE x IS NOT NULL)"
    )

    return {
        'pk_cols_s_aliased': pk_cols_s_aliased,
        'pk_cols_t_aliased': pk_cols_t_aliased,
        'pk_select_coalesce': pk_select_coalesce,
        'join_on_aliased': join_on_aliased,
        'first_pk_s_alias': first_pk_s_alias,
        'first_pk_t_alias': first_pk_t_alias,
        'comp_cols_s_aliased': comp_cols_s_aliased,
        'comp_cols_t_aliased': comp_cols_t_aliased,
        'original_cols_s_aliased': original_cols_s_aliased,
        'original_cols_t_aliased': original_cols_t_aliased,
        'mismatch_predicate_sql': mismatch_predicate_sql,
        'diff_details_structs_sql': diff_details_structs_sql
    }


# --- Helper Function to Generate Filter Clauses ---
def generate_filter_clauses(
    filters: Optional[List[str]],
    source_cols_schema: Dict[str, Any],
    target_cols_schema: Dict[str, Any]
) -> tuple[str, str]:
    """
    Generate filter clauses for flexible SQL-like filtering.

    Args:
        filters: List of SQL-like filter conditions
        source_cols_schema: Source table schema
        target_cols_schema: Target table schema

    Returns:
        Tuple of (source_filter_clause, target_filter_clause)
    """
    source_filter_clause_str = ""
    target_filter_clause_str = ""

    if filters:
        # Join multiple filter conditions with AND
        filter_conditions = " AND ".join(filters)

        # Apply same filters to both source and target tables
        # Add table aliases to avoid column name conflicts
        source_filter_clause_str = f"WHERE {filter_conditions}"
        target_filter_clause_str = f"WHERE {filter_conditions}"

    return source_filter_clause_str, target_filter_clause_str


# --- Helper Function to Generate Schema Mismatch Query ---
def generate_schema_mismatch_query(
    source_table_fq: str,
    target_table_fq: str
) -> str:
    """
    Generate the schema mismatch query.

    Args:
        source_table_fq: Fully qualified source table name
        target_table_fq: Fully qualified target table name

    Returns:
        Formatted schema mismatch query
    """
    schema_mismatch_template = read_sql_file("get_schema_mismatches.sql")

    source_p, source_d, source_t_short = source_table_fq.split('.')
    target_p, target_d, target_t_short = target_table_fq.split('.')

    return schema_mismatch_template.format(
        source_project=source_p,
        source_dataset=source_d,
        source_table_name=source_t_short,
        target_project=target_p,
        target_dataset=target_d,
        target_table_name=target_t_short,
    )


# --- Helper Function to Generate Count Queries ---
def generate_count_queries(
    mismatch_report_table_fq: Optional[str],
    final_compare_cols_list: List[str],
    pk_columns_list: List[str],
    source_table_fq: str,
    target_table_fq: str,
    source_filter_clause_str: str,
    target_filter_clause_str: str,
    mismatch_predicate_sql: str
) -> Dict[str, str]:
    """
    Generate count queries for mismatched and missing records.

    Args:
        mismatch_report_table_fq: Optional mismatch report table name
        final_compare_cols_list: List of comparison columns
        pk_columns_list: List of primary key columns
        source_table_fq: Source table name
        target_table_fq: Target table name
        source_filter_clause_str: Source filter clause
        target_filter_clause_str: Target filter clause
        mismatch_predicate_sql: Mismatch predicate SQL

    Returns:
        Dictionary containing all count queries
    """
    base_count_substitutions = {
        "pk_columns_list_sql": ", ".join([f"`{col}`" for col in pk_columns_list]),
        "compare_columns_list_sql": (
            ", ".join([f"`{col}`" for col in final_compare_cols_list])) if final_compare_cols_list else "1 AS dummy_col",
        "source_table_fq": source_table_fq,
        "source_filter_clause": source_filter_clause_str.replace("s.", ""),
        "target_table_fq": target_table_fq,
        "target_filter_clause": target_filter_clause_str.replace("t.", ""),
        "join_on_clause_sql": " AND ".join([f"s.`{col}` = t.`{col}`" for col in pk_columns_list]),
        "first_pk_column": f"`{pk_columns_list[0]}`",
        "mismatch_predicate_sql": mismatch_predicate_sql.replace("s_cmp_", "s.").replace("t_cmp_", "t.").replace(
            "s_pk_", "s.").replace("t_pk_", "t."),  # Revert aliases
    }

    if mismatch_report_table_fq:
        count_mismatched_query = read_sql_file("count_mismatched_records.sql").format(
            mismatch_report_table_fq=mismatch_report_table_fq)
        count_missing_in_source_query = read_sql_file("count_missing_in_source.sql").format(
            mismatch_report_table_fq=mismatch_report_table_fq)
        count_missing_in_target_query = read_sql_file("count_missing_in_target.sql").format(
            mismatch_report_table_fq=mismatch_report_table_fq)
    else:  # Direct counts
        count_mismatched_query = read_sql_file(
            "count_mismatched_records_direct.sql").format(**base_count_substitutions)
        count_missing_in_source_query = read_sql_file(
            "count_missing_in_source_direct.sql").format(**base_count_substitutions)
        count_missing_in_target_query = read_sql_file(
            "count_missing_in_target_direct.sql").format(**base_count_substitutions)

    return {
        'count_mismatched_query': count_mismatched_query,
        'count_missing_in_source_query': count_missing_in_source_query,
        'count_missing_in_target_query': count_missing_in_target_query
    }
