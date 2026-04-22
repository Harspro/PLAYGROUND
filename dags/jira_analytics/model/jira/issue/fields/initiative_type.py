"""
This module defines the data class InitiativeType.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class InitiativeType:
    """
    This class defines the data structure JIRA Cloud use to specify "Custom Field (Initiative Type)".
    """
    # The value of this InitiativeType, e.g. "Business".
    value: str
