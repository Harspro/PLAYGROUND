"""
This module defines the Fields and related dataclasses.
"""

from dataclasses import dataclass
from typing import Optional

from jira_analytics.model.jira.issue.fields.initiative_category import InitiativeCategory
from jira_analytics.model.jira.issue.fields.initiative_type import InitiativeType
from jira_analytics.model.jira.issue.fields.link import Link, EpicLink
from jira_analytics.model.jira.issue.fields.resolution import Resolution
from jira_analytics.model.jira.issue.fields.status import Status
from jira_analytics.model.jira.issue.fields.t_shirt_size import TShirtSize
from jira_analytics.model.jira.issue.fields.overall_status import OverallStatus


@dataclass(frozen=True)
class Fields:
    """
    This class defines the data structure of the issue fields received from Jira cloud.
    """
    # The summary of the issue.
    summary: str
    # The status of the issue, in "IssueStatus" format.
    status: Status
    # The date the issue was created
    created: str


@dataclass(frozen=True)
class InitiativeFields(Fields):
    """
    This class extends the Fields class for when the issues are initiatives.
    """

    # The due date of the initiative
    duedate: Optional[str] = None
    # The "Custom Field (T-Shirt Size)".
    customfield_13165: Optional[TShirtSize] = None
    # The start date of the initiative
    customfield_12956: Optional[str] = None
    # The "Custom Field (Initiative Category)".
    customfield_13540: Optional[InitiativeCategory] = None
    # The "Custom Field (Initiative Type)".
    customfield_13645: Optional[InitiativeType] = None
    # The "Custom Field (Overall Status)
    customfield_13677: Optional[OverallStatus] = None


@dataclass(frozen=True)
class EpicFields(Fields):
    """
    This class extends the Fields class for when the issues are epics.
    """

    # The link to parent issue, in "ParentLink" format, if applicable.
    parent: Optional[Link]
    # The "Custom Field (Epic Link)", in EpicLink format.
    customfield_10102: Optional[EpicLink]
    # The "Custom Field (ParentInitiativeID)"
    customfield_13565: Optional[str] = None
    # The "Custom Field (T-Shirt Size)".
    customfield_13165: Optional[TShirtSize] = None
    # The resolution of the issue, in "Resolution" format.
    resolution: Optional[Resolution] = None


@dataclass(frozen=True)
class StoryFields(Fields):
    """
    This class extends the Fields class for when the issues are stories.
    """

    # The link to parent issue, in "ParentLink" format, if applicable.
    parent: Optional[Link]
    # The "Custom Field (Epic Link)", in EpicLink format.
    customfield_10102: Optional[EpicLink]
    # The "Custom Field (Story Point)".
    customfield_10117: Optional[float] = None
    # The "Custom Field (ParentInitiativeID)"
    customfield_13565: Optional[str] = None
    # The resolution of the issue, in "Resolution" format.
    resolution: Optional[Resolution] = None
