"""
This module defines the data class TShirtSize.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class TShirtSize:
    """
    This class defines the data structure JIRA Cloud use to specify "Custom Field (T-Shirt Size)".
    It is the sizing technique PCB use to estimate user story in agile projects.
    """
    # The value of this T-Shirt size, e.g. "Medium".
    value: str
