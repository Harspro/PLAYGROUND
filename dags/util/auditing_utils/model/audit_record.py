"""
This file defines the data class AuditRecord.
"""
from dataclasses import dataclass


@dataclass
class AuditRecord:
    """
    This file defines the data structure for the configuration of an AuditRecord.
    """
    # The dataset that the audit table belongs to.
    dataset_id: str

    # The table id to put records into.
    table_id: str

    # Values to insert into the audit table. This is one string where all the values are comma separated.
    audit_record_values: str
