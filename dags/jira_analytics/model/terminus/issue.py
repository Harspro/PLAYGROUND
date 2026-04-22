"""
This module defines the data class Issue and related classes.
"""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Issue:
    """
    The dataclass Issue defines the shared interface of the landing zone dataclasses.
    """
    # The unique identifier of the issue.
    issue_key: str
    # The summary/title of the epic
    summary: str
    # The status of the epic.
    status: str
    # The date the initiative was created
    created_date: datetime
    # The time stamp at which the JQL was queried.
    query_time: datetime


@dataclass(frozen=True)
class Initiative(Issue):
    """
    The dataclass Initiative defines the schema used by the table storing properties of initiatives
    in the landing zone.
    """

    # The T-shirt sizing of the initiative.
    t_shirt_size: str
    # The due date of the initiative.
    due_date: datetime
    # The start date of the initiative.
    start_date: datetime
    # The Initiative Category of the initiative.
    initiative_category: str
    # The Initiative Type of the initiative.
    initiative_type: str
    # The overall status of the initiative
    overall_status: str


@dataclass(frozen=True)
class Epic(Issue):
    """
    The dataclass Epic defines the schema used by the table storing properties of epics
    in the landing zone.
    """

    # The T-shirt sizing of the epic.
    t_shirt_size: str
    # The key of the initiative associated with the epic.
    initiative_key: str
    # The issue's resolution.
    resolution: str


@dataclass(frozen=True)
class Story(Issue):
    """
    The dataclass Story defines the schema used by the table storing properties of stories
    in the landing zone.
    """

    # The unique identifier of the epic which the story is a part of.
    epic_key: str
    # The sizing of the story.
    story_point: int
    # The issue's resolution.
    resolution: str
