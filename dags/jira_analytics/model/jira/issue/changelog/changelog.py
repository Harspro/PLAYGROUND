"""
This module defines the data class ChangeLog.
"""

from dataclasses import dataclass
from typing import List

from jira_analytics.model.jira.issue.changelog.event.event import Event


@dataclass(frozen=True)
class ChangeLog:
    """
    This data class defines the data structure used by Jira Cloud to represent
    changes made to a Jira issue.
    """

    # The start point of the collection to return.
    startAt: int

    # The total number of records in the collection.
    maxResults: int

    # The total number of logs available.
    total: int = None

    # The list of edit events to the Jira issue.
    histories: List[Event] = ()
