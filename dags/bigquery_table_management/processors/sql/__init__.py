"""
SQL processor for BigQuery Table Management Framework.

This module contains:
    - SQLProcessor: Executes SQL DDL scripts as versioned operations
"""

from bigquery_table_management.processors.sql.sql_processor import SQLProcessor

__all__ = ["SQLProcessor"]
