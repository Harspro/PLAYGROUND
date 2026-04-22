"""
This module defines the data class Overall Status
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class OverallStatus:
    """
    This class defines the data structure JIRA Cloud use to specify "Custom Field (Overall Status)".
    """
    # The value of Overall Status e.g. "Minor Delay".
    value: str
