"""
This module defines the Issue and related dataclasses.
"""

from dataclasses import dataclass
from math import ceil
from typing import Optional

from jira_analytics.model.jira.issue.changelog.changelog import ChangeLog
from jira_analytics.model.jira.issue.fields.fields import Fields, InitiativeFields, EpicFields, StoryFields


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
    fields: Fields

    @property
    def summary(self) -> str:
        return self.fields.summary

    @property
    def status(self) -> str:
        return self.fields.status.name

    @property
    def issue_key(self) -> str:
        return self.key

    @property
    def created_date(self) -> str:
        return self.fields.created


@dataclass(frozen=True)
class Initiative(Issue):
    """
    This class extends the Issue class for when the issues are initiatives.
    """
    # The list of fields of the initiative.
    fields: InitiativeFields

    @property
    def t_shirt_size(self) -> Optional[str]:
        if self.fields.customfield_13165:
            return self.fields.customfield_13165.value
        else:
            return None

    @property
    def due_date(self) -> Optional[str]:
        return self.fields.duedate

    @property
    def start_date(self) -> Optional[str]:
        return self.fields.customfield_12956

    @property
    def initiative_category(self) -> Optional[str]:
        if self.fields.customfield_13540:
            return self.fields.customfield_13540.value
        else:
            return None

    @property
    def initiative_type(self) -> Optional[str]:
        if self.fields.customfield_13645:
            return self.fields.customfield_13645.value
        else:
            return None

    @property
    def overall_status(self) -> Optional[str]:
        if self.fields.customfield_13677:
            return self.fields.customfield_13677.value
        else:
            return None


@dataclass(frozen=True)
class Epic(Issue):
    """
    This class extends the Issue class for when the issues are epics.
    """
    # The list of fields of the initiative.
    fields: EpicFields

    @property
    def t_shirt_size(self) -> Optional[str]:
        if self.fields.customfield_13165:
            return self.fields.customfield_13165.value
        else:
            return None

    @property
    def initiative_key(self) -> str:
        if self.fields.parent:
            return self.fields.parent.key
        elif self.fields.customfield_13565:
            return self.fields.customfield_13565
        elif self.fields.customfield_10102 and self.fields.customfield_10102.data:
            return self.fields.customfield_10102.data.key
        else:
            return "[UNKNOWN]"

    @property
    def resolution(self) -> Optional[str]:
        if self.fields.resolution:
            return self.fields.resolution.name
        else:
            return None


@dataclass(frozen=True)
class Story(Issue):
    """
    This class extends the Issue class for when the issues are stories.
    """
    # The list of fields of the initiative.
    fields: StoryFields

    @property
    def epic_key(self) -> str:
        if self.fields.parent:
            return self.fields.parent.key
        elif self.fields.customfield_13565:
            return self.fields.customfield_13565
        elif self.fields.customfield_10102 and self.fields.customfield_10102.data:
            return self.fields.customfield_10102.data.key
        else:
            return "[UNKNOWN]"

    @property
    def story_point(self) -> float:
        if self.fields.customfield_10117 is None:
            return 0.0
        else:
            return float(ceil(self.fields.customfield_10117))

    @property
    def resolution(self) -> Optional[str]:
        if self.fields.resolution:
            return self.fields.resolution.name
        else:
            return None
