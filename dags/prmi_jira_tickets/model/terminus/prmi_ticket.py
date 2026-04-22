"""
This module defines the data class PRMITicket for the landing zone.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class PRMITicket:
    """
    The dataclass PRMITicket defines the schema used by the table storing properties of PRMI tickets
    in the landing zone.
    """
    # The unique identifier of the issue.
    issue_key: str
    # The summary/title of the PRMI ticket
    summary: str
    # The status of the PRMI ticket.
    status: str
    # The date the PRMI ticket was created
    created_date: datetime
    # The priority ID (1 for P1, 2 for P2, etc.)
    priority_id: str
    # The priority name
    priority_name: str
    # The time stamp at which the JQL was queried.
    query_time: datetime
    # Standard fields
    assignee: Optional[str] = None
    description: Optional[str] = None
    issuetype: Optional[str] = None
    reporter: Optional[str] = None
    # The impacted capabilities as a comma-separated string
    impacted_capabilities: Optional[str] = None
    # The impacted platforms as a comma-separated string
    impacted_platforms: Optional[str] = None
    # Custom fields - Time and Status
    time_in_status: Optional[str] = None  # customfield_10101
    time_to_resolution: Optional[str] = None  # customfield_11000
    # Custom fields - Incident Details
    impact_description: Optional[str] = None  # customfield_12911
    resolution_detail: Optional[str] = None  # customfield_12974
    duration: Optional[str] = None  # customfield_13122
    incident_id: Optional[str] = None  # customfield_13123
    affected_systems: Optional[str] = None  # customfield_13173
    incident_start_date: Optional[datetime] = None  # customfield_13174 (Incident Start Date)
    incident_end_date: Optional[datetime] = None  # customfield_13175 (Incident End Date)
    issue_actual_start: Optional[datetime] = None  # customfield_13243 (Issue Actual Start)
    end_of_issue_resolution: Optional[datetime] = None  # customfield_13244 (End of Issue - No More Work Required)
    end_of_customer_impact_recovery: Optional[datetime] = None  # customfield_13245 (End of Customer Impact - Recovery)
    # Custom fields - Root Cause and Actions
    mi_root_cause: Optional[str] = None  # customfield_13246
    corrective_action: Optional[str] = None  # customfield_13247
    incident_type: Optional[str] = None  # customfield_13248
    systemic_problem: Optional[str] = None  # customfield_13250 (array as comma-separated)
    never_call: Optional[str] = None  # customfield_13251 (array as comma-separated)
    # Custom fields - SNOW Integration
    snow_problem_id: Optional[str] = None  # customfield_13266
    snow_change_id: Optional[str] = None  # customfield_13267
    caused_by_change: Optional[str] = None  # customfield_13268 (array as comma-separated)
    # Custom fields - RCA and Control
    pending_rca: Optional[str] = None  # customfield_13285 (array as comma-separated)
    within_control: Optional[str] = None  # customfield_13286 (array as comma-separated)
    # Custom fields - Channels and Reporting
    application_channel: Optional[str] = None  # customfield_13295 (array as comma-separated)
    orm_reporting: Optional[str] = None  # customfield_13302 (array as comma-separated)
    sla_breached: Optional[str] = None  # customfield_13337
    # Custom fields - Vendors
    impacted_vendor: Optional[str] = None  # customfield_13340 (array as comma-separated)
    caused_by_vendor: Optional[str] = None  # customfield_13377 (array as comma-separated)
    # Custom fields - Response and Detection
    issue_response_time: Optional[datetime] = None  # customfield_13376
    sre_task_required: Optional[str] = None  # customfield_13470
    issue_reported_date_time: Optional[datetime] = None  # customfield_13586
    detection: Optional[str] = None  # customfield_13589
    effort_spent_by_support_team: Optional[str] = None  # customfield_13648
    update_field: Optional[str] = None  # customfield_14681
    # Custom fields - Banner Times
    banner_start_time: Optional[datetime] = None  # customfield_14682
    banner_end_time: Optional[datetime] = None  # customfield_14683
    # Custom fields - Incident Cause
    incident_cause: Optional[str] = None  # customfield_15078 (array as comma-separated)
    # Custom fields - Task Requirements
    platforms_task_required: Optional[str] = None  # customfield_16246
    engineering_task_required: Optional[str] = None  # customfield_16247
