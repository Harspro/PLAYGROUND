"""
Constants for BigQuery Table Management Framework.

Defines string literals used for YAML configuration keys and other framework constants.
"""

from typing import Final

# YAML Configuration Keys
CLUSTERING_FIELDS: Final[str] = "clustering_fields"
PARTITION_FIELD: Final[str] = "partition_field"
TRANSFORMATIONS: Final[str] = "transformations"
