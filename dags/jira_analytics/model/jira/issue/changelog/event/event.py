"""
This module defines the `Event` data class.
"""

from dataclasses import dataclass
from typing import List

from jira_analytics.model.jira.issue.changelog.event.item import Item


@dataclass(frozen=True)
class Event:
    """
    The Event is the data structure used by Jira Cloud to represent an
    edit event to a Jira issue.
    """

    # The timestamp at which the edit event occurred, in the format of "%Y-%m-%dT%H:%M:%S.%f%z"
    created: str
    # The list of actions taken in this edit event.
    items: List[Item]

    @property
    def event_time(self) -> str:
        return self.created
