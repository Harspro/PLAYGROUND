"""
This module defines the data class `ChangelogCount`.
"""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class ChangelogCount:
    """
    The ChangelogCount is the data structure representing the total number
    of changelog that occurred in JIRA.
    """

    # The key of the issue.
    issue_key: str
    # The total count of changelog under the issue
    count: int
    # The time when the data was queried.
    query_time: datetime
