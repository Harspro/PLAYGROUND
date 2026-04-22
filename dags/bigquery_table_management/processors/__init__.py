"""
Processors for BigQuery Table Management Framework.

Processors are specialized classes that apply specific operations
(e.g., clustering updates, partitioning changes) to BigQuery tables.
They are invoked by handlers and are not part of the handler routing system.

All processors extend BaseProcessor which provides common workflow
and initialization.
"""

from bigquery_table_management.processors.base_processor import BaseProcessor

__all__ = ["BaseProcessor"]
