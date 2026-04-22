"""
Test cases for table_comparator utils module.
"""
import pytest
from unittest.mock import patch, MagicMock, mock_open
from datetime import datetime
from google.cloud.exceptions import NotFound
from google.cloud import bigquery

from table_comparator.utils import (
    validate_date_format,
    validate_table_name_format,
    validate_expressions,
    build_column_expression,
    build_column_expression_with_alias,
    get_data_type_category,
    get_column_expressions,
    read_sql_file,
    execute_bigquery_job,
    validate_input_parameters,
    get_table_schemas,
    determine_comparison_columns,
    generate_sql_aliases,
    generate_filter_clauses,
    generate_schema_mismatch_query,
    generate_count_queries
)


class TestValidationFunctions:
    """Test cases for validation functions."""

    @pytest.mark.parametrize("date_str,expected", [
        ("2024-01-01", True),
        ("2024-12-31", True),
        ("2024-02-29", True),  # Leap year
        ("2024-13-01", False),  # Invalid month
        ("2024-01-32", False),  # Invalid day
        ("2024/01/01", False),  # Wrong format
        ("", False),            # Empty string
        ("invalid", False),     # Invalid string
    ])
    def test_validate_date_format(self, date_str, expected):
        """Test validate_date_format function."""
        result = validate_date_format(date_str)
        assert result == expected

    @pytest.mark.parametrize("table_name,expected", [
        ("project.dataset.table", True),
        ("test-project.test_dataset.test_table", True),
        ("project.dataset", False),  # Missing table
        ("project.dataset.table.extra", False),  # Too many parts
        ("project", False),  # Too few parts
        ("", False),  # Empty string
    ])
    def test_validate_table_name_format(self, table_name, expected):
        """Test validate_table_name_format function."""
        result = validate_table_name_format(table_name)
        assert result == expected

    def test_validate_expressions_safe(self):
        """Test validate_expressions with safe expressions."""
        safe_expressions = [
            "TRIM(column)",
            "COALESCE(column, '')",
            "UPPER(column)",
            "LOWER(column)"
        ]

        # Should not raise any exception
        validate_expressions(safe_expressions, "test_column")

    def test_validate_expressions_unsafe(self):
        """Test validate_expressions with unsafe expressions."""
        unsafe_expressions = [
            "DROP TABLE column",
            "DELETE FROM column",
            "INSERT INTO column",
            "UPDATE column SET",
            "CREATE TABLE column",
            "ALTER TABLE column"
        ]

        for expr in unsafe_expressions:
            with pytest.raises(ValueError, match="Unsafe expression detected"):
                validate_expressions([expr], "test_column")

    def test_validate_expressions_missing_placeholder(self):
        """Test validate_expressions with missing 'column' placeholder."""
        expressions = ["UPPER(test_col)"]

        # Should log warning but not raise exception
        validate_expressions(expressions, "test_column")


class TestColumnExpressionFunctions:
    """Test cases for column expression functions."""

    def test_build_column_expression_no_expressions(self):
        """Test build_column_expression with no expressions."""
        result = build_column_expression("test_column", [])
        assert result == "`test_column`"

    def test_build_column_expression_single_expression(self):
        """Test build_column_expression with single expression."""
        expressions = ["TRIM(column)"]
        result = build_column_expression("test_column", expressions)
        assert result == "TRIM(`test_column`)"

    def test_build_column_expression_multiple_expressions(self):
        """Test build_column_expression with multiple expressions."""
        expressions = ["TRIM(column)", "UPPER(column)", "COALESCE(column, '')"]
        result = build_column_expression("test_column", expressions)
        expected = "COALESCE(UPPER(TRIM(`test_column`)), '')"
        assert result == expected

    def test_build_column_expression_with_alias(self):
        """Test build_column_expression_with_alias function."""
        expressions = ["TRIM(column)", "UPPER(column)"]
        result = build_column_expression_with_alias(
            "test_column", expressions, "s")
        expected = "UPPER(TRIM(s.`test_column`))"
        assert result == expected


class TestDataTypeFunctions:
    """Test cases for data type functions."""

    @pytest.mark.parametrize("data_type,expected", [
        ("STRING", "string_types"),
        ("VARCHAR", "string_types"),
        ("CHAR", "string_types"),
        ("TEXT", "string_types"),
        ("INT64", "numeric_types"),
        ("INTEGER", "numeric_types"),
        ("FLOAT64", "numeric_types"),
        ("FLOAT", "numeric_types"),
        ("NUMERIC", "numeric_types"),
        ("DECIMAL", "numeric_types"),
        ("DATE", "date_types"),
        ("DATETIME", "date_types"),
        ("TIMESTAMP", "date_types"),
        ("BOOL", "boolean_types"),
        ("BOOLEAN", "boolean_types"),
        ("UNKNOWN_TYPE", None),
        ("", None),
    ])
    def test_get_data_type_category(self, data_type, expected):
        """Test get_data_type_category function."""
        result = get_data_type_category(data_type)
        assert result == expected

    def test_get_column_expressions_no_expressions(self):
        """Test get_column_expressions with no expressions."""
        result = get_column_expressions("test_column")
        assert result == []

    def test_get_column_expressions_column_specific(self):
        """Test get_column_expressions with column-specific expressions."""
        column_expressions = {
            "test_column": {
                "expressions": ["TRIM(column)", "UPPER(column)"]
            }
        }
        result = get_column_expressions(
            "test_column", column_expressions=column_expressions)
        assert result == ["TRIM(column)", "UPPER(column)"]

    def test_get_column_expressions_global_by_type(self):
        """Test get_column_expressions with global type-based expressions."""
        source_schema = {"test_column": MagicMock(field_type="STRING")}
        target_schema = {"test_column": MagicMock(field_type="STRING")}
        global_expressions = {
            "string_types": ["COALESCE(column, '')", "TRIM(column)"]
        }

        result = get_column_expressions(
            "test_column",
            source_cols_schema=source_schema,
            target_cols_schema=target_schema,
            global_expressions_by_type=global_expressions
        )
        assert result == ["COALESCE(column, '')", "TRIM(column)"]

    def test_get_column_expressions_column_specific_overrides_global(self):
        """Test that column-specific expressions override global ones."""
        column_expressions = {
            "test_column": {
                "expressions": ["CUSTOM(column)"]
            }
        }
        source_schema = {"test_column": MagicMock(field_type="STRING")}
        target_schema = {"test_column": MagicMock(field_type="STRING")}
        global_expressions = {
            "string_types": ["COALESCE(column, '')"]
        }

        result = get_column_expressions(
            "test_column",
            column_expressions=column_expressions,
            source_cols_schema=source_schema,
            target_cols_schema=target_schema,
            global_expressions_by_type=global_expressions
        )
        assert result == ["CUSTOM(column)"]


class TestFileOperations:
    """Test cases for file operations."""

    @patch('builtins.open', new_callable=mock_open,
           read_data="SELECT * FROM test_table")
    def test_read_sql_file(self, mock_file):
        """Test read_sql_file function."""
        result = read_sql_file("test.sql")
        assert result == "SELECT * FROM test_table"
        mock_file.assert_called_once()

    @patch('builtins.open', side_effect=FileNotFoundError("File not found"))
    def test_read_sql_file_not_found(self, mock_file):
        """Test read_sql_file function with file not found."""
        with pytest.raises(FileNotFoundError):
            read_sql_file("nonexistent.sql")


class TestBigQueryOperations:
    """Test cases for BigQuery operations."""

    @patch('table_comparator.utils.bigquery.Client')
    def test_execute_bigquery_job_empty_query(self, mock_client_class):
        """Test execute_bigquery_job with empty query."""
        result = execute_bigquery_job("", fetch_results=True)
        assert result == []


class TestInputParameterValidation:
    """Test cases for input parameter validation functions."""

    def test_validate_input_parameters_valid(self):
        """Test validate_input_parameters with valid parameters."""
        # Should not raise any exception
        validate_input_parameters(
            source_table_fq="project.dataset.source",
            target_table_fq="project.dataset.target",
            pk_columns_list=["id", "date"]
        )

    def test_validate_input_parameters_invalid_table_format(self):
        """Test validate_input_parameters with invalid table format."""
        with pytest.raises(ValueError, match="Source table name 'invalid_table' must be in format 'project.dataset.table'"):
            validate_input_parameters(
                source_table_fq="invalid_table",
                target_table_fq="project.dataset.target",
                pk_columns_list=["id"]
            )

    def test_validate_input_parameters_empty_pk(self):
        """Test validate_input_parameters with empty primary key."""
        with pytest.raises(ValueError, match="Primary key columns list cannot be empty."):
            validate_input_parameters(
                source_table_fq="project.dataset.source",
                target_table_fq="project.dataset.target",
                pk_columns_list=[]
            )

    def test_validate_input_parameters_with_filters(self):
        """Test validate_input_parameters with filters (should not affect validation)."""
        # Filters don't affect the basic validation, so this should pass
        validate_input_parameters(
            source_table_fq="project.dataset.source",
            target_table_fq="project.dataset.target",
            pk_columns_list=["id"]
        )


class TestSchemaFunctions:
    """Test cases for schema functions."""

    @patch('table_comparator.utils.bigquery.Client')
    def test_get_table_schemas_not_found(self, mock_client_class):
        """Test get_table_schemas with table not found."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_table.side_effect = NotFound("Table not found")

        with pytest.raises(NotFound):
            get_table_schemas(
                "project.dataset.source",
                "project.dataset.target")


class TestComparisonFunctions:
    """Test cases for comparison functions."""

    def test_determine_comparison_columns_user_specified(self):
        """Test determine_comparison_columns with user-specified columns."""
        source_schema = {"col1": MagicMock(), "col2": MagicMock()}
        target_schema = {"col1": MagicMock(), "col2": MagicMock()}

        result = determine_comparison_columns(
            user_compare_cols_list=["col1"],
            source_cols_schema=source_schema,
            target_cols_schema=target_schema,
            pk_columns_list=["id"]
        )

        assert result == ["col1"]

    def test_determine_comparison_columns_auto_detect(self):
        """Test determine_comparison_columns with auto-detection."""
        source_schema = {
            "col1": MagicMock(),
            "col2": MagicMock(),
            "id": MagicMock()}
        target_schema = {
            "col1": MagicMock(),
            "col2": MagicMock(),
            "id": MagicMock()}

        result = determine_comparison_columns(
            user_compare_cols_list=None,
            source_cols_schema=source_schema,
            target_cols_schema=target_schema,
            pk_columns_list=["id"]
        )

        assert "col1" in result
        assert "col2" in result
        assert "id" not in result  # PK should be excluded

    def test_determine_comparison_columns_with_exclusions(self):
        """Test determine_comparison_columns with excluded columns."""
        source_schema = {
            "col1": MagicMock(),
            "col2": MagicMock(),
            "id": MagicMock()}
        target_schema = {
            "col1": MagicMock(),
            "col2": MagicMock(),
            "id": MagicMock()}

        result = determine_comparison_columns(
            user_compare_cols_list=None,
            source_cols_schema=source_schema,
            target_cols_schema=target_schema,
            pk_columns_list=["id"],
            exclude_columns_list=["col1"]
        )

        assert "col1" not in result
        assert "col2" in result
        assert "id" not in result

    def test_determine_comparison_columns_column_not_found(self):
        """Test determine_comparison_columns with column not found in both tables."""
        source_schema = {"col1": MagicMock()}
        target_schema = {"col2": MagicMock()}

        with pytest.raises(ValueError, match="Comparison column 'col1' not found"):
            determine_comparison_columns(
                user_compare_cols_list=["col1"],
                source_cols_schema=source_schema,
                target_cols_schema=target_schema,
                pk_columns_list=["id"]
            )


class TestSQLGenerationFunctions:
    """Test cases for SQL generation functions."""

    def test_generate_sql_aliases_basic(self):
        """Test generate_sql_aliases with basic configuration."""
        result = generate_sql_aliases(
            pk_columns_list=["id"],
            final_compare_cols_list=["name", "value"]
        )

        assert "pk_cols_s_aliased" in result
        assert "pk_cols_t_aliased" in result
        assert "pk_select_coalesce" in result
        assert "join_on_aliased" in result
        assert "comp_cols_s_aliased" in result
        assert "comp_cols_t_aliased" in result

    def test_generate_sql_aliases_with_expressions(self):
        """Test generate_sql_aliases with column expressions."""
        column_expressions = {"name": {"expressions": ["TRIM(column)"]}}
        source_schema = {"name": MagicMock(field_type="STRING")}
        target_schema = {"name": MagicMock(field_type="STRING")}

        result = generate_sql_aliases(
            pk_columns_list=["id"],
            final_compare_cols_list=["name"],
            column_expressions=column_expressions,
            source_cols_schema=source_schema,
            target_cols_schema=target_schema
        )

        assert "comp_cols_s_aliased" in result
        assert "comp_cols_t_aliased" in result

    def test_generate_filter_clauses_with_filters(self):
        """Test generate_filter_clauses with flexible filtering."""
        source_schema = {"status": MagicMock(), "amount": MagicMock()}
        target_schema = {"status": MagicMock(), "amount": MagicMock()}

        source_filter, target_filter = generate_filter_clauses(
            filters=["status = 'ACTIVE'", "amount > 1000"],
            source_cols_schema=source_schema,
            target_cols_schema=target_schema
        )

        expected_filter = "WHERE status = 'ACTIVE' AND amount > 1000"
        assert source_filter == expected_filter
        assert target_filter == expected_filter

    def test_generate_filter_clauses_no_filters(self):
        """Test generate_filter_clauses without filters."""
        source_schema = {"id": MagicMock()}
        target_schema = {"id": MagicMock()}

        source_filter, target_filter = generate_filter_clauses(
            filters=None,
            source_cols_schema=source_schema,
            target_cols_schema=target_schema
        )

        assert source_filter == ""
        assert target_filter == ""

    def test_generate_filter_clauses_single_filter(self):
        """Test generate_filter_clauses with single filter."""
        source_schema = {"status": MagicMock()}
        target_schema = {"status": MagicMock()}

        source_filter, target_filter = generate_filter_clauses(
            filters=["status = 'ACTIVE'"],
            source_cols_schema=source_schema,
            target_cols_schema=target_schema
        )

        expected_filter = "WHERE status = 'ACTIVE'"
        assert source_filter == expected_filter
        assert target_filter == expected_filter

    def test_generate_filter_clauses_complex_conditions(self):
        """Test generate_filter_clauses with complex filter conditions."""
        source_schema = {
            "status": MagicMock(),
            "amount": MagicMock(),
            "created_date": MagicMock()}
        target_schema = {
            "status": MagicMock(),
            "amount": MagicMock(),
            "created_date": MagicMock()}

        filters = [
            "status IN ('ACTIVE', 'PENDING')",
            "amount BETWEEN 100 AND 10000",
            "created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)"
        ]

        source_filter, target_filter = generate_filter_clauses(
            filters=filters,
            source_cols_schema=source_schema,
            target_cols_schema=target_schema
        )

        expected_filter = "WHERE status IN ('ACTIVE', 'PENDING') AND amount BETWEEN 100 AND 10000 AND created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)"
        assert source_filter == expected_filter
        assert target_filter == expected_filter

    def test_generate_filter_clauses_date_based_filter(self):
        """Test generate_filter_clauses with date-based filter (replacing old date filtering)."""
        source_schema = {"created_date": MagicMock()}
        target_schema = {"created_date": MagicMock()}

        source_filter, target_filter = generate_filter_clauses(
            filters=["created_date = CURRENT_DATE()"],
            source_cols_schema=source_schema,
            target_cols_schema=target_schema
        )

        expected_filter = "WHERE created_date = CURRENT_DATE()"
        assert source_filter == expected_filter
        assert target_filter == expected_filter

    @patch('table_comparator.utils.read_sql_file')
    def test_generate_schema_mismatch_query(self, mock_read_sql):
        """Test generate_schema_mismatch_query function."""
        mock_read_sql.return_value = "SELECT * FROM schema_mismatches WHERE source_project='project'"

        result = generate_schema_mismatch_query(
            "project.dataset.source",
            "project.dataset.target"
        )
        assert "project" in result

        mock_read_sql.assert_called_once_with("get_schema_mismatches.sql")

    @patch('table_comparator.utils.read_sql_file')
    def test_generate_count_queries_with_report_table(self, mock_read_sql):
        """Test generate_count_queries with mismatch report table."""
        mock_read_sql.side_effect = [
            "SELECT COUNT(*) FROM {mismatch_report_table_fq}",
            "SELECT COUNT(*) FROM {mismatch_report_table_fq} WHERE missing_in_source",
            "SELECT COUNT(*) FROM {mismatch_report_table_fq} WHERE missing_in_target"]

        result = generate_count_queries(
            mismatch_report_table_fq="test.report.table",
            final_compare_cols_list=["col1", "col2"],
            pk_columns_list=["id"],
            source_table_fq="source.table",
            target_table_fq="target.table",
            source_filter_clause_str="WHERE 1=1",
            target_filter_clause_str="WHERE 1=1",
            mismatch_predicate_sql="col1 != col2"
        )

        assert "count_mismatched_query" in result
        assert "count_missing_in_source_query" in result
        assert "count_missing_in_target_query" in result

    @patch('table_comparator.utils.read_sql_file')
    def test_generate_count_queries_without_report_table(self, mock_read_sql):
        """Test generate_count_queries without mismatch report table."""
        mock_read_sql.side_effect = [
            "SELECT COUNT(*) FROM direct_mismatched",
            "SELECT COUNT(*) FROM direct_missing_source",
            "SELECT COUNT(*) FROM direct_missing_target"
        ]

        result = generate_count_queries(
            mismatch_report_table_fq=None,
            final_compare_cols_list=["col1", "col2"],
            pk_columns_list=["id"],
            source_table_fq="source.table",
            target_table_fq="target.table",
            source_filter_clause_str="WHERE 1=1",
            target_filter_clause_str="WHERE 1=1",
            mismatch_predicate_sql="col1 != col2"
        )

        assert "count_mismatched_query" in result
        assert "count_missing_in_source_query" in result
        assert "count_missing_in_target_query" in result
