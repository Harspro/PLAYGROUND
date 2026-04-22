"""
This module defines the data class SprintIssue
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class SprintIssue:
    """
    The dataclass SprintIssue defines the structure for sprint issue-related data.
    """
    # The unique identifier of the issue.
    issue_key: str
    # A brief description or summary of the issue.
    summary: str
    # The estimated story points for the issue.
    story_points: float
    # The ID of the sprint to which this issue belongs.
    sprint_id: int
    # The type of issue (e.g., bug, story, task, spike).
    issue_type: str
