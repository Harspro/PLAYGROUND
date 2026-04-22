"""
This module defines the data class `Item`.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Item:
    """
    This class is the data structure used by Jira Cloud to represent
    the change to a field that occurred during a changelog event.
    """

    # The name of the field
    field: str

    # The type of the field
    fieldtype: str

    # The id of the field
    fieldId: Optional[str]

    # The previous value of the field
    fromString: Optional[str]

    # The new value of the field
    toString: Optional[str]

    @property
    def field_type(self) -> str:
        return self.fieldtype

    @property
    def field_id(self) -> Optional[str]:
        return self.fieldId

    @property
    def from_string(self) -> Optional[str]:
        return self.fromString

    @property
    def to_string(self) -> Optional[str]:
        return self.toString
