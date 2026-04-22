"""
This module defines the data class InitiativeCategory.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class InitiativeCategory:
    """
    This class defines the data structure JIRA Cloud use to specify "Custom Field (Initiative Category)".
    """
    # The value of this InitiativeCategory, e.g. "Pebble".
    value: str
