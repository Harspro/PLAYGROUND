"""
This module defines the data class `ChangelogEvent`.
"""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class ChangelogEvent:
    """
    The ChangelogEvent is the data structure representing a changelog event that
    occurred in JIRA.
    """

    # The key of the issue.
    issue_key: str
    # The date time of the changelog entry
    event_time: datetime
    # The name of the edited field
    field: str
    # The type of the edited field
    field_type: str
    # The id of the edited field
    field_id: str
    # The former value of the edited field
    from_string: str
    # The new value of the edited field
    to_string: str
    # The time when the data was queried.
    query_time: datetime
