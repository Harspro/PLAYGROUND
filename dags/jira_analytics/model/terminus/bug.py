"""
This module defines the data class Bug
"""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Bug:
    """
    The dataclass Bug defines the structure for sprint-related data.
    """

    # The unique identifier of the issue.
    issue_key: str
    # The name of the Bug.
    summary: str
    # The state of the Bug (e.g., closed, active).
    state: str
    # Key of the parent.
    parent_key: str
    # The estimated story points for the Bug.
    story_points: float
    # The Name of the Sprint to which the Bug belongs.
    sprint_name: str
    # The Env Type  of the Bug ( eg..PROD, UAT etc)
    env_type: str
    # The Team/pod name
    team_name: str
    # The priority of the Bug
    priority: str
    # The created date of the Bug.
    created_date: datetime
