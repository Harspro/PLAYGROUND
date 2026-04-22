"""
Purge List Exclusion file and data validation.

This module provides functions to:
1. Validate data using SQL queries for required columns, date format and mandatory fields
2. Generate comprehensive validation reports
"""

import logging
from typing import List, Dict, Any
from google.cloud import bigquery
from airflow.exceptions import AirflowFailException

logger = logging.getLogger(__name__)


class PurgeListExclusionFileValidation:
    """Utility class for BigQuery data validation operations."""

    def __init__(self, project_id: str):
        """
        Initialize the validation utilities.

        Args:
            project_id: GCP project ID
        """
        self.project_id = project_id
        self.client = bigquery.Client()

    def validate_required_columns(
        self,
        table_id: str,
        required_columns: List[str]
    ) -> Dict[str, Any]:
        """
        Validate that all required columns exist in the table.

        Args:
            table_id: Full table ID
            required_columns: List of required column names

        Returns:
            Validation result dictionary
        """
        try:
            logger.info(f"""Validating required columns for table: {table_id}
                            Required columns: {required_columns}""")

            # Get table schema
            table = self.client.get_table(table_id)
            existing_columns = [field.name for field in table.schema]

            # Check for missing columns
            missing_columns = [col for col in required_columns if col not in existing_columns]

            validation_result = {
                "table_id": table_id,
                "required_columns": required_columns,
                "existing_columns": existing_columns,
                "missing_columns": missing_columns,
                "is_valid": len(missing_columns) == 0
            }

            if missing_columns:
                error_msg = f"Missing required columns: {missing_columns}"
                validation_result["error_message"] = error_msg
            else:
                logger.info("All required columns are present")

            return validation_result

        except Exception as e:
            logger.error(f"Failed to validate required columns for {table_id}: {str(e)}")
            raise AirflowFailException(f"Column validation failed: {str(e)}")

    def validate_mandatory_fields(
        self,
        table_id: str,
        mandatory_fields: List[str]
    ) -> Dict[str, Any]:
        """
        Validate that mandatory fields are not null or empty.

        Args:
            table_id: Full table ID
            mandatory_fields: List of mandatory field names

        Returns:
            Validation result dictionary
        """
        try:
            logger.info(f"""Validating mandatory fields for table: {table_id}
                            Mandatory fields: {mandatory_fields}""")

            # Build validation query
            null_checks = []
            empty_checks = []

            for field in mandatory_fields:
                null_checks.append(f"{field} IS NULL")
                empty_checks.append(f"TRIM({field}) = ''")

            validation_query = f"""
            SELECT
                COUNT(*) as total_rows,
                SUM(CASE WHEN {' OR '.join(null_checks)} THEN 1 ELSE 0 END) as null_violations,
                SUM(CASE WHEN {' OR '.join(empty_checks)} THEN 1 ELSE 0 END) as empty_violations,
                SUM(CASE WHEN {' OR '.join(null_checks + empty_checks)} THEN 1 ELSE 0 END) as total_violations
            FROM `{table_id}`
            """

            logger.info(f"Executing validation query: {validation_query}")

            # Execute validation query
            query_job = self.client.query(validation_query)
            results = list(query_job.result())

            if not results:
                raise AirflowFailException("Validation query returned no results")

            result = results[0]

            validation_result = {
                "table_id": table_id,
                "mandatory_fields": mandatory_fields,
                "total_rows": result.total_rows,
                "null_violations": result.null_violations,
                "empty_violations": result.empty_violations,
                "total_violations": result.total_violations,
                "is_valid": result.total_violations == 0
            }

            if result.total_violations > 0:
                error_msg = f"Found {result.total_violations} violations in mandatory fields"
                validation_result["error_message"] = error_msg
            else:
                logger.info("All mandatory fields are valid")

            return validation_result

        except Exception as e:
            logger.error(f"Failed to validate mandatory fields for {table_id}: {str(e)}")
            raise AirflowFailException(f"Mandatory field validation failed: {str(e)}")

    def _build_dob_format_query(
        self,
        table_id: str,
        dob_column: str
    ) -> str:
        """
        Build SQL query for DOB format validation.

        Args:
            table_id: Full table ID
            dob_column: Name of the DOB column (INTEGER datatype)

        Returns:
            SQL query string
        """
        # Build format validation for INTEGER datatype (YYYYMMDD format)
        # Check if the integer is 8 digits and represents a valid date
        format_check = f"""
            CASE
                WHEN {dob_column} >= 10000101 AND {dob_column} <= 99991231 THEN
                    CASE
                        WHEN SAFE.PARSE_DATE('%Y%m%d', CAST({dob_column} AS STRING)) IS NOT NULL THEN 0
                        ELSE 1
                    END
                ELSE 1
            END
        """

        query = f"""
        SELECT
            COUNT(*) as total_rows,
            SUM({format_check}) as invalid_format_count,
            SUM({format_check}) as total_violations
        FROM `{table_id}`
        WHERE {dob_column} IS NOT NULL
        """

        return query

    def validate_dob_format(
        self,
        table_id: str,
        dob_column: str,
        expected_format: str = "YYYYMMDD"
    ) -> Dict[str, Any]:
        """
        Validate Date of Birth (DOB) format and age constraints.

        Args:
            table_id: Full table ID
            dob_column: Name of the DOB column
            expected_format: Expected date format YYYYMMDD

        Returns:
            Validation result dictionary
        """
        try:
            logger.info(f"""Validating DOB format for table: {table_id}
                            DOB column: {dob_column}
                            Expected format: {expected_format}""")

            # Build validation query based on expected format
            format_validation_query = self._build_dob_format_query(table_id, dob_column)

            logger.info(f"Executing DOB validation query: {format_validation_query}")

            # Execute validation query
            query_job = self.client.query(format_validation_query)
            results = list(query_job.result())

            if not results:
                raise AirflowFailException("DOB validation query returned no results")

            result = results[0]

            validation_result = {
                "table_id": table_id,
                "dob_column": dob_column,
                "expected_format": expected_format,
                "total_rows": result.total_rows,
                "invalid_format_count": result.invalid_format_count,
                "total_violations": result.total_violations,
                "is_valid": result.total_violations == 0
            }

            if result.total_violations > 0:
                error_msg = f"Found {result.total_violations} DOB validation violations"
                validation_result["error_message"] = error_msg
            else:
                logger.info("All DOB values are valid")

            return validation_result

        except Exception as e:
            logger.error(f"Failed to validate DOB format for {table_id}: {str(e)}")
            raise AirflowFailException(f"DOB validation failed: {str(e)}")

    def get_violation_details(
        self,
        table_id: str,
        mandatory_fields: List[str],
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get detailed information about validation violations.

        Args:
            table_id: Full table ID
            mandatory_fields: List of mandatory field names
            limit: Maximum number of violation records to return

        Returns:
            List of violation records
        """
        try:
            logger.info(f"Getting violation details for table: {table_id}")

            # Build query to get violation details
            null_checks = []
            empty_checks = []

            for field in mandatory_fields:
                null_checks.append(f"{field} IS NULL")
                empty_checks.append(f"TRIM({field}) = ''")

            violation_query = f"""
            SELECT
                *,
                CASE
                    WHEN {' OR '.join(null_checks)} THEN 'NULL_VIOLATION'
                    WHEN {' OR '.join(empty_checks)} THEN 'EMPTY_VIOLATION'
                    ELSE 'UNKNOWN'
                END as violation_type
            FROM `{table_id}`
            WHERE {' OR '.join(null_checks + empty_checks)}
            LIMIT {limit}
            """

            logger.info("Executing violation details query")

            # Execute query
            query_job = self.client.query(violation_query)
            results = list(query_job.result())

            violation_records = []
            for row in results:
                violation_records.append(dict(row))

            logger.info(f"Found {len(violation_records)} violation records")

            return violation_records

        except Exception as e:
            logger.error(f"Failed to get violation details for {table_id}: {str(e)}")
            raise AirflowFailException(f"Failed to get violation details: {str(e)}")

    def perform_file_validation(
        self,
        table_id: str,
        required_columns: List[str],
        mandatory_fields: List[str],
        dob_column: str,
        dob_expected_format: str,
        include_violation_details: bool = True
    ) -> Dict[str, Any]:
        """
        Perform file validation including column and field validation.

        Args:
            table_id: Full table ID
            required_columns: List of required column names
            mandatory_fields: List of mandatory field names
            dob_column: dob column name
            dob_expected_format: dob expected date format
            include_violation_details: Whether to include detailed violation records

        Returns:
            Comprehensive validation result
        """
        try:
            logger.info(f"Starting file validation for table: {table_id}")

            # Validate required columns
            required_column_validation = self.validate_required_columns(table_id, required_columns)

            # If column validation fails, return early
            if not required_column_validation["is_valid"]:
                return {
                    "table_id": table_id,
                    "overall_valid": False,
                    "column_validation": required_column_validation,
                    "field_validation": None,
                    "violation_details": []
                }

            # Validate mandatory fields
            mandatory_column_validation = self.validate_mandatory_fields(table_id, mandatory_fields)

            # Get violation details if requested and there are violations
            violation_details = []
            if include_violation_details and not mandatory_column_validation["is_valid"]:
                violation_details = self.get_violation_details(table_id, mandatory_fields)

            # Validate DOB format
            dob_validation = self.validate_dob_format(table_id, dob_column, dob_expected_format)

            # Determine overall validation status
            overall_valid = required_column_validation["is_valid"] and mandatory_column_validation["is_valid"] and dob_validation["is_valid"]

            validation_result = {
                "table_id": table_id,
                "overall_valid": overall_valid,
                "column_validation": required_column_validation,
                "field_validation": mandatory_column_validation,
                "violation_details": violation_details,
                "dob_validation": dob_validation
            }

            return validation_result

        except Exception as e:
            logger.error(f"File validation failed for {table_id}: {str(e)}")
            raise AirflowFailException(f"File validation failed: {str(e)}")
