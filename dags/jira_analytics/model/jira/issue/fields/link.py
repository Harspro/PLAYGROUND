"""
This module defines the data structure used to represent an issue's link to its
parent issue.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Link:
    """
    This class defines the data structure used by Jira cloud to represent the link between
    an issue and its parent issue.
    """

    # The parent issue's key.
    key: str


@dataclass(frozen=True)
class EpicLink:
    """
    This class defines the data structure used by Jira cloud to represent the link between
    an issue and its parent issue in custom field `customfield_10102`
    """

    # The link to its parent. If a parent link do not exist, this field is set to None.
    data: Optional[Link] = None
