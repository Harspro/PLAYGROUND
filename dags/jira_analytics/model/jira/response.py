"""
This module defines the Response and related dataclasses.
"""

from dataclasses import dataclass
from typing import List

from jira_analytics.model.jira.issue.changelog.event.event import Event
from jira_analytics.model.jira.issue.issue import Issue, Initiative, Epic, Story
from jira_analytics.model.terminus.sprint import Sprint


@dataclass(frozen=True)
class Response:
    """
    This class defines the data structure received in the response from Jira cloud.
    """
    # Boolean indicating whether this response is the last page
    isLast: bool


@dataclass(frozen=True)
class ChangelogResponse(Response):
    """
    This class extends the Response class for when the responses are changelogs.
    """
    # The list of Changelog Events.
    values: List[Event]


@dataclass(frozen=True)
class IssueResponse(Response):
    """
    This class extends the Response class for when the responses are issues.
    """
    # The list of Jira Issues.
    issues: List[Issue]


@dataclass(frozen=True)
class SprintResponse(Response):
    """
    This class extends the Response class for when the responses are sprints.
    """
    # The list of Jira Sprints.
    values: List[Sprint]


@dataclass(frozen=True)
class InitiativeResponse(IssueResponse):
    """
    This class extends the IssueResponse class for when the issues are initiatives.
    """
    # The list of Jira Initiatives.
    issues: List[Initiative]


@dataclass(frozen=True)
class EpicResponse(IssueResponse):
    """
    This class extends the IssueResponse class for when the issues are epics.
    """
    # The list of Jira Initiatives.
    issues: List[Epic]


@dataclass(frozen=True)
class StoryResponse(IssueResponse):
    """
    This class extends the IssueResponse class for when the issues are stories.
    """
    # The list of Jira Initiatives.
    issues: List[Story]
