"""
This module defines the data class `Status`.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Status:
    """
    This class defines the data structure used by JIRA cloud to represent an JIRA issue's status.
    """

    # The name of the status.
    name: str
