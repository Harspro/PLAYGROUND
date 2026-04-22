"""
This module defines the Issue and related dataclasses.
"""

from dataclasses import dataclass
from typing import Optional, List

from prmi_jira_tickets.model.jira.issue.changelog.changelog import ChangeLog
from prmi_jira_tickets.model.jira.issue.fields.fields import PRMITicketFields


@dataclass(frozen=True)
class Issue:
    """
    The Issue class defines the data structure of issues received from Jira cloud.
    """
    # The key to the issue.
    key: str
    # The log of edit events for this issue.
    changelog: ChangeLog
    # The fields of the issue.
    fields: PRMITicketFields

    @property
    def summary(self) -> str:
        if self.fields is None or self.fields.summary is None:
            return ""
        return self.fields.summary

    @property
    def status(self) -> str:
        if self.fields is None or self.fields.status is None:
            return ""
        return self.fields.status.name

    @property
    def issue_key(self) -> str:
        return self.key

    @property
    def created_date(self) -> str:
        if self.fields is None or self.fields.created is None:
            return ""
        return self.fields.created

    @property
    def priority_id(self) -> str:
        if self.fields is None or self.fields.priority is None:
            return ""
        return self.fields.priority.id

    @property
    def priority_name(self) -> str:
        if self.fields is None or self.fields.priority is None:
            return ""
        return self.fields.priority.name


@dataclass(frozen=True)
class PRMITicket(Issue):
    """
    This class extends the Issue class for when the issues are PRMI tickets.
    """
    # The list of fields of the PRMI ticket.
    fields: PRMITicketFields

    @property
    def impacted_capabilities(self) -> Optional[List[str]]:
        """
        Returns a list of impacted capability values.
        """
        if self.fields.customfield_13289:
            return [cap.value for cap in self.fields.customfield_13289]
        return None

    @property
    def impacted_platforms(self) -> Optional[List[str]]:
        """
        Returns a list of impacted platform values.
        """
        if self.fields.customfield_15144:
            return [platform.value for platform in self.fields.customfield_15144]
        return None

    @property
    def incident_end_date(self) -> Optional[str]:
        """
        Returns the incident end date if available.
        Uses customfield_13175 (Incident End Date).
        """
        return self.fields.customfield_13175 if hasattr(self.fields, 'customfield_13175') else None
