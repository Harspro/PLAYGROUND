"""
This module defines the data class `Resolution`.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Resolution:
    """
    This class defines the data structure used by JIRA cloud to represent an JIRA issue's resolution.
    """

    # The name of the resolution.
    name: str
