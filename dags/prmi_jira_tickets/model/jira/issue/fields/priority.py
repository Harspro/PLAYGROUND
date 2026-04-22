"""
This module defines the data class `Priority`.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Priority:
    """
    This class defines the data structure used by JIRA cloud to represent an JIRA issue's priority.
    """

    # The id of the priority (1 for P1, 2 for P2, etc.).
    id: str

    # The name of the priority.
    name: str
