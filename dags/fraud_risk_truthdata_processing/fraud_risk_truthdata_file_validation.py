"""
Fraud Risk Truth Data file and data validation.

This module provides functions to:
1. Validate file format (CSV)
2. Validate required columns
3. Validate mandatory fields (non-null, non-empty)
"""

import logging
from typing import List, Dict, Any
from google.cloud import bigquery
from airflow.exceptions import AirflowFailException

logger = logging.getLogger(__name__)


class FraudRiskTruthDataFileValidation:
    """Utility class for BigQuery data validation operations."""

    def __init__(self, project_id: str):
        """
        Initialize the validation utilities.

        Args:
            project_id: GCP project ID
        """
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)

    def validate_file_format(self, file_uri: str) -> Dict[str, Any]:
        """
        Validate that the file is a CSV format.

        Args:
            file_uri: GCS URI of the file

        Returns:
            Validation result dictionary
        """
        try:
            logger.info(f"Validating file format for: {file_uri}")

            # Check file extension
            if not file_uri.lower().endswith('.csv'):
                return {
                    "file_uri": file_uri,
                    "is_valid": False,
                    "error_message": f"File format validation failed: Expected CSV file, got {file_uri.split('.')[-1]}"
                }

            logger.info("File format validation passed: CSV format detected")
            return {
                "file_uri": file_uri,
                "is_valid": True
            }

        except Exception as e:
            logger.error(f"Failed to validate file format for {file_uri}: {str(e)}")
            raise AirflowFailException(f"File format validation failed: {str(e)}")

    def validate_required_columns(
        self,
        table_id: str,
        required_columns: List[str]
    ) -> Dict[str, Any]:
        """
        Validate that all required columns exist in the table.
        Uses case-insensitive matching to handle CSV column name variations.

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
            existing_columns_lower = [col.lower() for col in existing_columns]

            # Check for missing columns (case-insensitive)
            missing_columns = []
            for req_col in required_columns:
                if req_col not in existing_columns and req_col.lower() not in existing_columns_lower:
                    missing_columns.append(req_col)

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
                logger.info("All required columns are present (case-insensitive match)")

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
        Uses case-insensitive column matching to handle CSV column name variations.

        Args:
            table_id: Full table ID
            mandatory_fields: List of mandatory field names

        Returns:
            Validation result dictionary
        """
        try:
            logger.info(f"""Validating mandatory fields for table: {table_id}
                            Mandatory fields: {mandatory_fields}""")

            # Get actual column names from table to match case-insensitively
            table = self.client.get_table(table_id)
            existing_columns = {col.name.lower(): col.name for col in table.schema}

            # Build validation query with actual column names
            null_checks = []
            empty_checks = []

            for field in mandatory_fields:
                # Find matching column (case-insensitive)
                actual_field = existing_columns.get(field.lower(), field)
                null_checks.append(f"`{actual_field}` IS NULL")
                empty_checks.append(f"TRIM(CAST(`{actual_field}` AS STRING)) = ''")

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
                error_msg = f"Found {result.total_violations} violations in mandatory fields (null: {result.null_violations}, empty: {result.empty_violations})"
                validation_result["error_message"] = error_msg

            else:
                logger.info("All mandatory fields are valid")

            return validation_result

        except Exception as e:
            logger.error(f"Failed to validate mandatory fields for {table_id}: {str(e)}")
            raise AirflowFailException(f"Mandatory field validation failed: {str(e)}")

    def get_violation_details(
        self,
        table_id: str,
        mandatory_fields: List[str],
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get detailed information about validation violations.
        Uses case-insensitive column matching to handle CSV column name variations.

        Args:
            table_id: Full table ID
            mandatory_fields: List of mandatory field names
            limit: Maximum number of violation records to return

        Returns:
            List of violation records
        """
        try:
            logger.info(f"Getting violation details for table: {table_id}")

            # Get actual column names from table to match case-insensitively
            table = self.client.get_table(table_id)
            existing_columns = {col.name.lower(): col.name for col in table.schema}

            # Build query to get violation details
            null_checks = []
            empty_checks = []

            for field in mandatory_fields:
                # Find matching column (case-insensitive)
                actual_field = existing_columns.get(field.lower(), field)
                null_checks.append(f"`{actual_field}` IS NULL")
                empty_checks.append(f"TRIM(CAST(`{actual_field}` AS STRING)) = ''")

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
        include_violation_details: bool = True
    ) -> Dict[str, Any]:
        """
        Perform file validation including column and field validation.

        Args:
            table_id: Full table ID
            required_columns: List of required column names
            mandatory_fields: List of mandatory field names
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

            # Determine overall validation status
            overall_valid = required_column_validation["is_valid"] and mandatory_column_validation["is_valid"]

            validation_result = {
                "table_id": table_id,
                "overall_valid": overall_valid,
                "column_validation": required_column_validation,
                "field_validation": mandatory_column_validation,
                "violation_details": violation_details
            }

            return validation_result

        except Exception as e:
            logger.error(f"File validation failed for {table_id}: {str(e)}")
            raise AirflowFailException(f"File validation failed: {str(e)}")
