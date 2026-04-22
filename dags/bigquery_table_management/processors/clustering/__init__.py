"""
Clustering processor and utilities for BigQuery Table Management Framework.

This module contains:
    - ClusteringProcessor: Applies clustering updates as versioned operations
    - clustering_utils: Low-level utilities for clustering operations
"""

from bigquery_table_management.processors.clustering.clustering_processor import ClusteringProcessor

__all__ = ["ClusteringProcessor"]
