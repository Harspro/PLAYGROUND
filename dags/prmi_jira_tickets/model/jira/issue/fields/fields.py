"""
This module defines the Fields and related dataclasses.
"""

from dataclasses import dataclass
from typing import Optional, List

from prmi_jira_tickets.model.jira.issue.fields.priority import Priority
from prmi_jira_tickets.model.jira.issue.fields.status import Status


@dataclass(frozen=True)
class CapabilityOption:
    """
    This class defines the data structure for a capability option.
    """
    value: str


@dataclass(frozen=True)
class PlatformOption:
    """
    This class defines the data structure for a platform option.
    """
    value: str


@dataclass(frozen=True)
class Fields:
    """
    This class defines the data structure of the issue fields received from Jira cloud.
    """
    # The summary of the issue.
    summary: str
    # The status of the issue, in "Status" format.
    status: Status
    # The date the issue was created
    created: str
    # The priority of the issue
    priority: Priority


@dataclass(frozen=True)
class PRMITicketFields(Fields):
    """
    This class extends the Fields class for when the issues are PRMI tickets.
    """
    # Standard fields (already in Fields base class: summary, status, created, priority)
    # Additional standard fields
    assignee: Optional[str] = None
    description: Optional[str] = None
    issuetype: Optional[str] = None
    reporter: Optional[str] = None

    # Custom fields
    customfield_10101: Optional[str] = None  # [CHART] Time in Status
    customfield_11000: Optional[str] = None  # Time to resolution
    customfield_12911: Optional[str] = None  # Impact Description
    customfield_12974: Optional[str] = None  # Resolution Detail
    customfield_13122: Optional[str] = None  # Duration
    customfield_13123: Optional[str] = None  # Incident ID
    customfield_13173: Optional[str] = None  # Affected Systems
    customfield_13174: Optional[str] = None  # Incident Start Date
    customfield_13175: Optional[str] = None  # Incident End Date (date field)
    customfield_13243: Optional[str] = None  # Issue Actual Start
    customfield_13244: Optional[str] = None  # End of Issue - No More Work Required (Resolution)
    customfield_13245: Optional[str] = None  # End of Customer Impact (Recovery)
    customfield_13246: Optional[str] = None  # MI Root Cause
    customfield_13247: Optional[str] = None  # Corrective Action
    customfield_13248: Optional[str] = None  # Incident Type
    customfield_13250: Optional[List] = None  # Systemic Problem (array)
    customfield_13251: Optional[List] = None  # NeverCall (array)
    customfield_13266: Optional[str] = None  # SNOW Problem ID
    customfield_13267: Optional[str] = None  # SNOW Change ID
    customfield_13268: Optional[List] = None  # Caused by Change (array)
    customfield_13285: Optional[List] = None  # Pending RCA (array)
    customfield_13286: Optional[List] = None  # Within Control (array)
    customfield_13289: Optional[List[CapabilityOption]] = None  # Impacted Capability (array)
    customfield_13295: Optional[List] = None  # Application Channel (array)
    customfield_13302: Optional[List] = None  # ORM Reporting (array)
    customfield_13337: Optional[str] = None  # SLA Breached (option)
    customfield_13340: Optional[List] = None  # Impacted Vendor (array)
    customfield_13376: Optional[str] = None  # Issue Response Time
    customfield_13377: Optional[List] = None  # Caused by Vendor (array)
    customfield_13470: Optional[str] = None  # SRE Task Required (option)
    customfield_13586: Optional[str] = None  # Issue Reported Date and Time
    customfield_13589: Optional[str] = None  # Detection
    customfield_13648: Optional[str] = None  # Effort spent by Support team
    customfield_14681: Optional[str] = None  # Update
    customfield_14682: Optional[str] = None  # Banner Start Time
    customfield_14683: Optional[str] = None  # Banner End Time
    customfield_15078: Optional[List] = None  # Incident Cause (array)
    customfield_15144: Optional[List[PlatformOption]] = None  # Impacted Platform (array)
    customfield_16246: Optional[str] = None  # Platforms Task Required (option)
    customfield_16247: Optional[str] = None  # Engineering Task Required (option)
