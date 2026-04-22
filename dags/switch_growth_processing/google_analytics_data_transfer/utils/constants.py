"""
Constants for GA4 migration / table copy with impersonation.

Config keys used by the impersonation DAG and config YAML.
Shared constants (BIGQUERY, PROJECT_ID, etc.) live in dags/util/constants.py.
"""
from typing import Final

# Config keys for table copy with impersonation (not in dags/util/constants.py)
AUDIT_TABLE_REF: Final = 'audit_table_ref'
SOURCE: Final = 'source'
LANDING: Final = 'landing'
VENDOR: Final = 'vendor'
CREATE_DISPOSITION: Final = 'create_disposition'
LOAD_TYPE: Final = 'load_type'
START_DATE: Final = 'start_date'
END_DATE: Final = 'end_date'
CHUNK_SIZE: Final = 'chunk_size'
LOAD_DATE: Final = 'load_date'  # dag_run.conf key for daily: single date to load (e.g. '2026-02-04')
LAST_SCHEDULED_RUN: Final = 22   # Last scheduled run for daily dags
SKIP_VENDOR_REPLICATION: Final = 'skip_vendor_replication'
