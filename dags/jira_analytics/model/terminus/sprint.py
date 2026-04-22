"""
This module defines the data class Sprint
"""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Sprint:
    """
    The dataclass Sprint defines the structure for sprint-related data.
    """
    # The unique identifier of the sprint.
    id: int
    # The name of the sprint.
    name: str
    # The state of the sprint (e.g., closed, active).
    state: str
    # The start date of the sprint.
    start_date: datetime
    # The end date of the sprint.
    end_date: datetime
    # The ID of the board to which the sprint belongs.
    board_id: int
