"""
This module defines the ChangeLog dataclass.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class ChangeLog:
    """
    This class defines the data structure of the changelog received from Jira cloud.
    """
    # The total number of changelog entries.
    total: int
